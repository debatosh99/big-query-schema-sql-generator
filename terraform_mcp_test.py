import requests
import json
import os
import sys
import asyncio
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession
import logging
import mcp_atlassian
import openai
from typing import List, Optional
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage, AIMessage
from mcp import McpError
import fastmcp
from typing import List, Dict, Any


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

os.environ["TRANSPORT_MODE"]="streamable-http"
os.environ["TRANSPORT_PORT"]="8080"
os.environ["TRANSPORT_HOST"]="127.0.0.1"
os.environ["TFE_TOKEN"]=""
os.environ["TFE_ADDRESS"]="https://app.terraform.io"
os.environ["TFE_SKIP_TLS_VERIFY"]="true"
os.environ["MCP_ENDPOINT"]="/mcp"
os.environ["MCP_SESSION_MODE"]="stateful"
os.environ["MCP_ALLOWED_ORIGINS"]="*"
os.environ["MCP_CORS_MODE"]="disabled"
os.environ["MCP_TLS_CERT_FILE"]="/path/to/cert.pem"
os.environ["MCP_TLS_KEY_FILE"]="/path/to/key.pem"


print("TFE Configuration:")
print(f"URL: {os.getenv('TFE_ADDRESS', 'Not set')}")


# The URL for the MCP server (streamable-http transport)
MCP_URL = "http://localhost:8080/mcp/"


async def list_mcp_tools():
    """Connect to MCP server and list available tools."""

    # Connect to the MCP server
    url = "http://localhost:8080/mcp"
    logger.info(f"Connecting to MCP server at {url}")

    try:
        async with streamablehttp_client(url) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                # Initialize the connection
                await session.initialize()

                # List available tools
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

                return tools

    except Exception as e:
        logger.error(f"Error connecting to MCP server: {e}")
        raise


def create_tool_selection_prompt(tools, query: str) -> str:
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
        prompt = create_tool_selection_prompt(tools, query)

        llm = ChatOpenAI(model="sonar-pro", api_key="",
                         base_url="https://api.perplexity.ai")
        response = llm.invoke([
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


# async def execute_tool(session, tool, query, parameters):
#     """Execute the selected tool with LLM-prepared parameters."""
#     try:
#         # Changed from execute to invoke
#         # result = await session.invoke(tool.name, {"query": query})
#         result = await session.call_tool(name=tool.name, arguments={"query": query})
#         # result = await session.read_resource("https://debatoshpradhan.atlassian.net/wiki/x/MIEF")
#         return result
#     except McpError as e:
#         logger.error(f"MCP protocol error while executing {tool.name}: {e}")
#         raise
#     except Exception as e:
#         logger.error(f"Error executing tool {tool.name}: {e}")
#         raise

# async def execute_tool(session, tool, query, parameters):
#     """Execute the selected tool with LLM-prepared parameters."""
#     try:
#         # Use the prepared parameters instead of hardcoding query
#         arguments = parameters if parameters else {"query": query}
#
#         # Call the tool with proper arguments
#         result = await session.call_tool(
#             name=tool.name,
#             arguments=arguments
#         )
#
#         # Check if the result has an error
#         if hasattr(result, 'isError') and result.isError:
#             logger.error(f"Tool execution error: {result}")
#             raise Exception(f"Tool execution failed: {result}")
#
#         return result.structuredContent if hasattr(result, 'structuredContent') else result
#
#     except McpError as e:
#         logger.error(f"MCP protocol error while executing {tool.name}: {e}")
#         raise
#     except Exception as e:
#         logger.error(f"Error executing tool {tool.name}: {e}")
#         raise


async def execute_tool_testing(session, tool, query, parameters) -> Dict[str, Any]:
    payload = {
        "tool_name": "confluence_search",
        "inputs": {"query": query}
    }
    async with session.post(f"http://localhost:8080/mcp/run_tool", json=payload) as response:
        return await response.json()

async def execute_tool(session, tool, query, parameters):
    """Execute the selected tool with LLM-prepared parameters."""
    try:
        logger.info(f"\nExecuting tool: {tool.name}")
        logger.info(f"Parameters: {parameters}")

        # Call the tool with the prepared parameters
        result = await session.call_tool(
            name=tool.name,
            arguments=parameters
        )

        # Enhanced logging for debugging
        logger.info(f"Tool response type: {type(result)}")
        logger.info(f"Raw tool response: {result}")

        # Handle potential error cases
        if hasattr(result, 'isError') and result.isError:
            error_content = result.content[0].text if result.content else "Unknown error"
            logger.error(f"Tool execution failed: {error_content}")
            raise Exception(f"Tool execution error: {error_content}")

        return result

    except Exception as e:
        logger.error(f"Error executing tool {tool.name}: {str(e)}", exc_info=True)
        raise

def interpret_response(tool_response):
    """Interpret and format the tool's response for the user."""
    try:
        if isinstance(tool_response, str):
            return tool_response
        return json.dumps(tool_response, indent=2)
    except Exception as e:
        logger.error(f"Error interpreting response: {e}")
        return str(tool_response)


async def process_query(query):
    """Process a user query using MCP tools."""
    url = "http://localhost:8080/mcp"

    try:
        async with streamablehttp_client(url) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()

                # Get available tools
                result = await session.list_tools()
                tools = result.tools

                # First use search_modules to find matching modules
                search_tool = next((t for t in tools if t.name == "search_modules"), None)
                if not search_tool:
                    raise Exception("search_modules tool not found")

                # Prepare search parameters
                search_params = {
                    "module_query": query,
                    "current_offset": 0
                }

                try:
                    # Execute search with refined query
                    search_response = await execute_tool(session, search_tool, query, search_params)
                    
                    if not search_response or not hasattr(search_response, 'content'):
                        return "No modules found matching your query."
                except McpError as e:
                    if "no modules found" in str(e).lower():
                        return "No modules found matching your query. Try using more specific keywords."
                    raise

                # Get the module_id from search response
                search_content = str(search_response.content)
                
                # Use LLM to extract module_id from search response
                llm = ChatOpenAI(model="sonar-pro", 
                               api_key="",
                               base_url="https://api.perplexity.ai")
                extract_prompt = f"""
                From this search response, extract the most relevant module_id:
                {search_content}
                
                Return only the module_id as a string, nothing else.
                """
                
                module_id_response = llm.invoke([HumanMessage(content=extract_prompt)])
                module_id = module_id_response.content.strip()

                # Now get module details
                details_tool = next((t for t in tools if t.name == "get_module_details"), None)
                if not details_tool:
                    return str(search_response.content)

                # Prepare details parameters
                details_params = {
                    "module_id": module_id
                }

                # Get module details
                details_response = await execute_tool(session, details_tool, query, details_params)

                # Combine and format responses
                if hasattr(details_response, 'content'):
                    return f"""
Search Results:
{str(search_response.content)}

Module Details:
{str(details_response.content)}
"""
                return str(search_response.content)

    except Exception as e:
        logger.error(f"Error in process_query: {str(e)}", exc_info=True)
        return f"Error processing query: {str(e)}"
    


def debug_tools():
    """Helper function to debug available tools."""
    async def run():
        url = "http://localhost:8080/mcp"
        async with streamablehttp_client(url) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.list_tools()
                print("\nAvailable Tools:")
                print("===============")
                for tool in result.tools:
                    print(f"\nTool: {tool.name}")
                    print(f"Description: {tool.description}")
                    print("Input Schema:")
                    print(json.dumps(tool.inputSchema, indent=2))
                    print("Output Schema:")
                    print(json.dumps(tool.outputSchema, indent=2))

    asyncio.run(run())



# async def process_query(query):
#     """Process a user query using MCP tools."""
#     url = "http://localhost:9000/mcp"
#
#     try:
#         async with streamablehttp_client(url) as (read_stream, write_stream, _):
#             async with ClientSession(read_stream, write_stream) as session:
#                 await session.initialize()
#
#                 # Get available tools
#                 result = await session.list_tools()
#                 tools = result.tools
#
#                 # For testing specific tool
#                 tool_name = "confluence_search"
#                 # tool_name = "search"
#                 selected_tool = next((tool for tool in tools if tool.name == tool_name), None)
#
#                 if not selected_tool:
#                     raise Exception(f"Tool {tool_name} not found")
#
#                 # Prepare parameters based on the tool's input schema
#                 parameters = {
#                     "query": query,
#                     # Add any additional required parameters based on the tool's input schema
#                 }
#
#                 # Execute tool with prepared parameters
#                 response = await execute_tool(session, selected_tool, query, parameters)
#
#                 # Interpret and return response
#                 return interpret_response(response)
#
#     except Exception as e:
#         logger.error(f"Error in process_query: {e}", exc_info=True)
#         return f"Error processing query: {str(e)}"

# async def process_query(query):
#     """Process a user query using MCP tools."""
#     url = "http://localhost:9000/mcp"
#
#     try:
#         async with streamablehttp_client(url) as (read_stream, write_stream, _):
#             async with ClientSession(read_stream, write_stream) as session:
#                 try:
#                     await session.initialize()
#
#                     # Get available tools
#                     result = await session.list_tools()
#                     tools = result.tools
#
#                     # # Select appropriate tool and get parameters
#                     # selected_tool, parameters = await select_tool(tools, query)
#                     # if not selected_tool:
#                     #     return "No suitable tool found for your query."
#
#                     # # Execute tool with prepared parameters
#                     # response = await execute_tool(session, selected_tool, query, parameters)
#
#                     required_tool = None
#                     for tool in tools:
#                         if tool.name == "confluence_search":
#                             print(f"\nTool: {tool.name}")
#                             print(f"Description: {tool.description}")
#                             print(f"Input Schema: {tool.inputSchema}")
#                             print(f"Output Schema: {tool.outputSchema}")
#                             required_tool = tool
#
#                     response = await execute_tool(session, required_tool, query, {})
#
#                     # Interpret and return response
#                     return interpret_response(response)
#
#                 except Exception as inner_e:
#                     logger.error(f"Error during session execution: {inner_e}", exc_info=True)
#                     raise
#                 finally:
#                     await session.close()
#
#     except Exception as e:
#         logger.error(f"Error in process_query: {e}", exc_info=True)
#         return f"Error processing query: {str(e)}"


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
#                 print("\nResponse:")
#                 print(response)
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


def main():
    """Main entry point for the script."""
    try:
        # First debug available tools
        print("\nDebugging available tools...")
        debug_tools()

        # Then try the query - use specific keywords for better search results
        query = "what are the mandatory variables in the terraform module google storage bucket"  # More specific, keyword-based query
        print(f"\nProcessing query: {query}")

        try:
            response = asyncio.run(process_query(query))
            if response.startswith("No modules found"):
                print("\nTrying alternative search...")
                # Try alternative search with different keywords
                alt_query = "gcp storage"
                response = asyncio.run(process_query(alt_query))
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            response = "Search failed. Please try with different keywords."
        print("\nResponse:")
        print(response)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"Error: {e}")


if __name__ == "__main__":
    main()







