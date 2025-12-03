from typing import Annotated, Any, Dict, List, Sequence, TypedDict
from langchain_core.tools import tool
from langchain_core.messages import BaseMessage, FunctionMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import add_messages, StateGraph, END
from langchain_core.messages import HumanMessage, AIMessageChunk, ToolMessage, SystemMessage, AIMessage
from pydantic import BaseModel
import httpx
from typing import Any, Literal
from langgraph.checkpoint.memory import MemorySaver
import asyncio
import logging
from uuid import uuid4
from langchain_groq import ChatGroq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class State(TypedDict):
    messages: Annotated[list, add_messages]



SYSTEM_INSTRUCTION = """
    'You are a specialized assistant for currency conversions. '
    "Your sole purpose is to use the 'get_exchange_rate' tool to answer questions about currency exchange rates. "
    'When using the get_exchange_rate tool, always use uppercase currency codes (e.g., USD, EUR, INR). '
    'Always validate currency codes before making the conversion. '
    'If the user asks about anything other than currency conversion or exchange rates, '
    'politely state that you cannot help with that topic and can only assist with currency-related queries. '
    'For each conversion request, first call get_exchange_rate, then format the response in a clear, user-friendly way. '
    'Do not attempt to answer unrelated questions or use tools for other purposes.'
"""

FORMAT_INSTRUCTION = """
    'Set response status to input_required if the user needs to provide more information to complete the request.'
    'Set response status to error if there is an error while processing the request.'
    'Set response status to completed if the request is complete.'
"""



@tool
def get_exchange_rate(
        currency_from: str = 'USD',
        currency_to: str = 'EUR',
        currency_date: str = 'latest',
):
    """Use this to get current exchange rate.

    Args:
        currency_from: The currency to convert from (e.g., USD). Must be a valid 3-letter currency code.
        currency_to: The currency to convert to (e.g., EUR). Must be a valid 3-letter currency code.
        currency_date: The date for the exchange rate or "latest". Defaults to "latest".

    Returns:
        A dictionary containing the exchange rate data, or an error message if the request fails.
    """
    # Validate currency codes
    currency_from = currency_from.upper()
    currency_to = currency_to.upper()

    if not (len(currency_from) == 3 and len(currency_to) == 3):
        return {'error': 'Currency codes must be 3 letters (e.g., USD, EUR, INR)'}

    try:
        response = httpx.get(
            f'https://api.frankfurter.app/{currency_date}',
            params={'from': currency_from, 'to': currency_to},
            timeout=10.0
        )
        response.raise_for_status()

        data = response.json()
        if 'rates' not in data:
            return {'error': 'Invalid API response format'}
        return data
    except httpx.HTTPError as e:
        return {'error': f'Invalid currency code or API request failed: {str(e)}'}
    except (ValueError, KeyError):
        return {'error': 'Invalid response from API'}
    except Exception as e:
        return {'error': f'An unexpected error occurred: {str(e)}'}


# Define response format
class ResponseFormat(BaseModel):
    """Respond to the user in this format."""

    status: Literal['input_required', 'completed', 'error'] = 'input_required'
    message: str



# Define the agent node - only LLM decision making
async def agent_node(state: State) -> Dict:
    """Agent node: Let the LLM decide whether to use tools."""
    messages = state["messages"]
    new_messages = [SystemMessage(content=SYSTEM_INSTRUCTION)]
    new_messages.extend(messages)
    
    tools = [get_exchange_rate]
    llm_with_tools = llm.bind_tools(tools=tools)
    
    # Let the LLM decide whether to use tools
    response = llm_with_tools.invoke(new_messages)
    
    # Add the assistant's response to messages
    messages.append(response)
    return {"messages": messages}


# Define the tool execution node
async def tool_node(state: State) -> Dict:
    """Tool node: Execute the tools that the LLM decided to call."""
    messages = state["messages"]
    
    # Get the last assistant message which should contain tool calls
    last_message = messages[-1]
    
    if not (hasattr(last_message, 'tool_calls') and last_message.tool_calls):
        return {"messages": messages}
    
    # Process each tool call
    for tool_call in last_message.tool_calls:
        tool_name = tool_call["name"]
        tool_input = tool_call["args"]
        
        if tool_name == "get_exchange_rate":
            tool_result = get_exchange_rate(tool_input)
        
        # Add tool result to messages
        messages.append(ToolMessage(
            content=str(tool_result),
            tool_call_id=tool_call["id"],
            name=tool_name
        ))
    
    return {"messages": messages}


# Define the response formatting node
async def response_node(state: State) -> Dict:
    """Response node: Format the final response with structured output."""
    messages = state["messages"]
    
    structured_llm = llm.with_structured_output(ResponseFormat)
    final_response = structured_llm.invoke(messages)
    messages.append(AIMessage(content=final_response.message))
    
    return {"messages": messages}


# Define routing function to decide if tool calls were made
def should_execute_tools(state: State) -> str:
    """Router: Check if the last message contains tool calls."""
    messages = state["messages"]
    last_message = messages[-1]
    
    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tool_node"
    return "response_node"




llm = ChatGroq(
    #model_name="meta-llama/llama-4-scout-17b-16e-instruct",
    model_name="llama-3.1-8b-instant",
    api_key="<>"
)

memory = MemorySaver()
graph_builder = StateGraph(State)

# Add nodes
graph_builder.add_node("agent", agent_node)
graph_builder.add_node("tool_execution", tool_node)
graph_builder.add_node("response_formatting", response_node)

# Set entry point
graph_builder.set_entry_point("agent")

# Add conditional edge: agent -> tool_execution or response_formatting
graph_builder.add_conditional_edges(
    "agent",
    should_execute_tools,
    {
        "tool_node": "tool_execution",
        "response_node": "response_formatting"
    }
)

# Add edge: tool_execution loops back to agent for re-evaluation
graph_builder.add_edge("tool_execution", "agent")

# Add edge: response_formatting -> END
graph_builder.add_edge("response_formatting", END)

graph = graph_builder.compile(checkpointer=memory)



async def main():
    """Main entry point for the script."""
    try:
        #query = "how much is 10 USD in INR?"
        query = "how much is 100 YEN in INR?"

        try:
            parent_thread_id = str(uuid4())
            initial_state = {
                "messages": [HumanMessage(content=query)]
            }
            response = await graph.ainvoke(input=initial_state, config={"configurable": {"thread_id": parent_thread_id}})
            print(f"\nResponse: {response['messages'][-1].content}\n")
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            print(f"\nError: {e}")

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())






