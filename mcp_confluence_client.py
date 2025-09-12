import json
import os
import asyncio
import aiohttp
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession
import logging
import openai
from typing import List, Optional
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage, AIMessage
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage, SystemMessage, ToolMessage
from langgraph.graph import StateGraph, END
from langchain_core.prompts import ChatPromptTemplate
from typing import Dict, Any, TypedDict, List, Annotated
from langgraph.graph import StateGraph, START, END, add_messages

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.environ["DOMAIN"] = "debatoshpradhan.atlassian.net"
os.environ["CONFLUENCE_URL"] = "https://debatoshpradhan.atlassian.net/wiki"
os.environ["CONFLUENCE_USERNAME"] = "xxxxx@gmail.com"
os.environ[
    "CONFLUENCE_API_TOKEN"] = "XXXXXX"

# Log Confluence configuration
logger.info("Confluence Configuration:")
logger.info(f"URL: {os.getenv('CONFLUENCE_URL', 'Not set')}")
logger.info(f"Username: {os.getenv('CONFLUENCE_USERNAME', 'Not set')}")
logger.info(f"API Token: {'Set' if os.getenv('CONFLUENCE_API_TOKEN') else 'Not set'}")

print("Confluence Configuration:")
print(f"URL: {os.getenv('CONFLUENCE_URL', 'Not set')}")
print(f"Username: {os.getenv('CONFLUENCE_USERNAME', 'Not set')}")
print(f"API Token: {'Set' if os.getenv('CONFLUENCE_API_TOKEN') else 'Not set'}")

# The URL for the MCP server (streamable-http transport)
MCP_URL = "http://localhost:9000/mcp/"


async def create_tool_selection_prompt(tools, query: str) -> str:
    """Create a prompt for the LLM to select the most appropriate tool and prepare input schema."""
    tools_description = "\n".join([
        f"Tool Name: {tool.name}\n"
        f"Description: {tool.description}\n"
        f"Input Schema: {tool.inputSchema}\n"
        f"---"
        for tool in tools
    ])

    prompt = f"""Given the following tools and user query, select the most appropriate tool and prepare its input parameters.
    Return your response in the following JSON format:
    {{
        "tool_name": "selected tool name",
        "parameters": {{
            // parameter values based on the tool's input schema
        }}
    }}

    Available Tools:
    {tools_description}

    User Query: {query}

    Response:"""

    return prompt


async def select_tool(tools, query: str) -> tuple[Optional[object], dict]:
    """Select the most appropriate tool using LLM and prepare input parameters."""
    try:
        prompt = await create_tool_selection_prompt(tools, query)

        llm = ChatOpenAI(model="sonar-pro", api_key="xxxxxx",
                         base_url="https://api.perplexity.ai")
        response = await llm.ainvoke([
            SystemMessage(
                content="You are a helpful assistant that selects tools and prepares their input parameters based on user queries."),
            HumanMessage(content=prompt)
        ])

        # Parse the JSON response
        try:
            llm_response = json.loads(response.content.strip())
            selected_tool_name = llm_response["tool_name"]
            parameters = llm_response["parameters"]

            # Find the tool object matching the selected name
            for tool in tools:
                if tool.name == selected_tool_name:
                    logger.info(f"Selected tool: {tool.name} with parameters: {parameters}")
                    return tool, parameters

            return None, {}

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing LLM response: {e}")
            return None, {}

    except Exception as e:
        logger.error(f"Error in LLM tool selection: {e}")
        return None, {}


async def execute_tool(session, tool, query, parameters):
    """Execute the selected tool with LLM-prepared parameters."""
    try:
        # Log tool details for debugging
        logger.info(f"Tool Details:")
        logger.info(f"Name: {tool.name}")
        logger.info(f"Description: {tool.description}")
        logger.info(f"Input Schema: {json.dumps(tool.inputSchema, indent=2)}")
        logger.info(f"Output Schema: {json.dumps(tool.outputSchema, indent=2)}")

        # # Format parameters according to the tool's input schema
        # formatted_parameters = {
        #     "query": query,
        #     # Add any additional required parameters from the tool's input schema
        #     "space": None,  # Only add if required by schema
        #     "limit": None   # Only add if required by schema
        # }
        #
        # # Remove None values
        # formatted_parameters = {k: v for k, v in formatted_parameters.items() if v is not None}

        # logger.info(f"Executing tool '{tool.name}' with parameters: {formatted_parameters}")

        # Format the arguments based on the tool's schema
        tool_arguments = {}
        if tool.name in ["search", "confluence_search"]:
            # For Confluence search, we need to properly structure the arguments
            tool_arguments = {
                "query": query,  # The search query
                "limit": 10,  # Maximum number of results to return
                "spaces_filter": None  # Optional: filter by specific spaces
            }
            logger.info(f"Sending search request with arguments: {tool_arguments}")
        else:
            tool_arguments = parameters

        result = await session.call_tool(
            name=tool.name,
            arguments=tool_arguments
        )

        # Enhanced logging for debugging
        logger.info(f"Tool response type: {type(result)}")
        logger.info(f"Raw tool response: {result}")

        # Handle potential error cases
        if hasattr(result, 'isError') and result.isError:
            error_content = result.content[0].text if result.content else "Unknown error"
            logger.error(f"Tool execution failed: {error_content}")
            raise Exception(f"Tool execution error: {error_content}")

        # Handle successful response
        try:
            if hasattr(result, 'content'):
                logger.info(f"Response content type: {type(result.content)}")
                logger.info(f"Response content: {result.content}")
            return result
        except Exception as e:
            logger.error(f"Error processing response: {str(e)}")
            raise

    except Exception as e:
        logger.error(f"Error executing tool {tool.name}: {str(e)}", exc_info=True)
        raise


async def interpret_response(tool_response):
    """Interpret and format the tool's response for the user."""
    try:
        if isinstance(tool_response, str):
            return tool_response

        # Handle CallToolResult object
        if hasattr(tool_response, 'content'):
            # Extract content from the response
            if isinstance(tool_response.content, list):
                # If content is a list, join the text from each item
                content = ' '.join(item.text for item in tool_response.content if hasattr(item, 'text'))
                return content
            elif hasattr(tool_response.content, 'text'):
                # If content has text attribute
                return tool_response.content.text
            else:
                # If content is something else, convert to string
                return str(tool_response.content)

        # For any other type of response
        return str(tool_response)
    except Exception as e:
        logger.error(f"Error interpreting response: {e}")
        return str(tool_response)


async def process_query(query):
    """Process a user query using MCP tools."""
    url = "http://localhost:9000/mcp"

    try:
        async with streamablehttp_client(url) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                try:
                    await session.initialize()

                    # Get available tools
                    result = await session.list_tools()
                    tools = result.tools

                    # Print tools in a readable format
                    print("\nAvailable MCP Tools:")
                    print("===================")
                    for tool in tools:
                        print(f"\nTool: {tool.name}")
                        print(f"Description: {tool.description}")
                        print(f"Input Schema: {tool.inputSchema}")
                        print(f"Output Schema: {tool.outputSchema}")

                    # Select appropriate tool and get parameters
                    selected_tool, parameters = await select_tool(tools, query)
                    if not selected_tool:
                        return "No suitable tool found for your query."

                    # Execute tool with prepared parameters
                    response = await execute_tool(session, selected_tool, query, parameters)

                    # Interpret and return response
                    return await interpret_response(response)

                except Exception as inner_e:
                    logger.error(f"Error during session execution: {inner_e}", exc_info=True)
                    raise


    except Exception as e:
        logger.error(f"Error in process_query: {e}", exc_info=True)
        return f"Error processing query: {str(e)}"


async def extract_page_text_contents(page_id):
    import aiohttp
    from aiohttp import BasicAuth
    # Replace these variables with your own details
    DOMAIN = os.environ.get("DOMAIN")  # For Atlassian Cloud
    PAGE_ID = page_id  # The Confluence page id
    EMAIL = os.environ.get("CONFLUENCE_USERNAME")  # Your Atlassian email
    API_TOKEN = os.environ.get("CONFLUENCE_API_TOKEN")  # Your Atlassian API token

    url = f"https://{DOMAIN}/wiki/rest/api/content/{PAGE_ID}?expand=body.storage"
    headers = {
        "Accept": "application/json",
    }
    auth = BasicAuth(EMAIL, API_TOKEN)

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, auth=auth) as response:
            if response.status == 200:
                page_json = await response.json()
                # The main content is in page_json['body']['storage']['value'], which is HTML
                html_content = page_json['body']['storage']['value']
                return html_content
            else:
                error_text = await response.text()
                print(f"Failed to fetch page. HTTP {response.status}: {error_text}")
                return ""


async def llm_refined_response(html_content, query):
    llm = ChatOpenAI(model="sonar-pro", api_key="xxxxxx",
                     base_url="https://api.perplexity.ai")
    system_message = SystemMessage(
        content="You are an AI agent and your task is to answer the user query based on the content provided")
    human_message = HumanMessage(content=f"My query : {query} \n\n and the context is : {html_content}")
    messages = [system_message, human_message]
    response = await llm.ainvoke(messages)
    return response.content





class AgentState(TypedDict):
    # messages: Annotated[list[str], add_messages]
    messages: List[BaseMessage]

async def process_confluence_mcp(state: AgentState) -> AgentState:
    """Use the confluence server capabilities to answer users question"""
    if len(state["messages"]) >= 1:
        human_request = state["messages"][-1].content
    response = await process_query(human_request)  # Remove asyncio.run here
    response = json.loads(response)
    page_id = response[0]["id"]
    title = response[0]["title"]
    html_content = await extract_page_text_contents(page_id)
    final_response = await llm_refined_response(html_content, human_request)
    print(final_response)
    state["messages"].append(AIMessage(content=final_response))
    return state

workflow = StateGraph(AgentState)
workflow.add_node("process_confluence_mcp", process_confluence_mcp)
workflow.set_entry_point("process_confluence_mcp")
workflow.add_edge("process_confluence_mcp", END)



############################################################################################

#--------------------------------------------------------------
from langgraph.checkpoint.memory import MemorySaver
checkpointer = MemorySaver()
graph = workflow.compile(checkpointer=checkpointer)
initial_state1 = {
    "messages": [HumanMessage(content="""what is the progress on hypersonic engine development""")]
}

async def generate():
    result1 = await graph.ainvoke(input=initial_state1, config={"configurable": {"thread_id": 105}})
    print("response1:", result1["messages"][-1].content)

import asyncio
print(asyncio.run(generate()))
#-------------------------------------------------------------




# def main():
#     """Main entry point for the script."""
#     try:
#         while True:
#             query = "what is the progress on hypersonic engine development"
#             if query.lower() == 'quit':
#                 break
#
#             try:
#                 response = asyncio.run(process_query(query))
#                 print("\nResponse..........:")
#                 response = json.loads(response)
#                 page_id = response[0]["id"]
#                 title = response[0]["title"]
#                 html_content = extract_page_text_contents(page_id)
#                 print(f"""{html_content}""")
#                 final_response = llm_refined_response(html_content, query)
#                 print(final_response)
#                 break
#             except Exception as e:
#                 logger.error(f"Error in main loop: {e}", exc_info=True)
#                 print(f"\nError: {e}")
#                 break
#
#     except KeyboardInterrupt:
#         print("\nOperation cancelled by user")
#     except Exception as e:
#         logger.error(f"Fatal error: {e}", exc_info=True)
#         print(f"Error: {e}")
#
# if __name__ == "__main__":
#     main()

