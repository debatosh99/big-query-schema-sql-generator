import requests
import json
import os
import sys
import asyncio
from mcp.client.streamable_http import streamablehttp_client
from mcp import ClientSession
import logging
import mcp_atlassian

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The URL for the MCP server (streamable-http transport)
MCP_URL = "http://localhost:9000/mcp/"


async def list_mcp_tools():
    """Connect to MCP server and list available tools."""

    # Connect to the MCP server
    url = "http://localhost:9000/mcp"
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


def main():
    """Main entry point for the script."""
    try:
        asyncio.run(list_mcp_tools())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()




