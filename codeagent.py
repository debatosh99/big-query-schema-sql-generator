from typing import Dict, Any, TypedDict
from langgraph.graph import Graph
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage
import os
import traceback
import ast

# Configuration
CODE_FILES_PATH = r"C:\your\python\files\path"  # Configure this path

class AgentState(TypedDict):
    error_trace: str
    code_content: Dict[str, str]
    analysis: str
    suggestion: str

def read_python_files(directory: str) -> Dict[str, str]:
    """Read all Python files from the configured directory."""
    code_files = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    code_files[file_path] = f.read()
    return code_files

def analyze_error(state: AgentState) -> AgentState:
    """Analyze the error trace and identify relevant files."""
    llm = ChatOpenAI(model="gpt-4")
    
    error_analysis = llm.invoke([
        HumanMessage(content=f"""
        Analyze this error trace and identify the key issues:
        {state['error_trace']}
        
        Related code files:
        {state['code_content']}
        """)
    ])
    
    state['analysis'] = error_analysis.content
    return state

def generate_suggestion(state: AgentState) -> AgentState:
    """Generate suggestions based on the analysis."""
    llm = ChatOpenAI(model="gpt-4")
    
    suggestion = llm.invoke([
        HumanMessage(content=f"""
        Based on this analysis:
        {state['analysis']}
        
        Provide specific suggestions to fix the code.
        """)
    ])
    
    state['suggestion'] = suggestion.content
    return state

def create_workflow() -> Graph:
    """Create the LangGraph workflow."""
    workflow = Graph()
    
    workflow.add_node("analyze_error", analyze_error)
    workflow.add_node("generate_suggestion", generate_suggestion)
    
    workflow.add_edge("analyze_error", "generate_suggestion")
    
    return workflow.compile()

def process_error(error_trace: str) -> Dict[str, Any]:
    """Process an error trace and return analysis and suggestions."""
    workflow = create_workflow()
    
    initial_state: AgentState = {
        "error_trace": error_trace,
        "code_content": read_python_files(CODE_FILES_PATH),
        "analysis": "",
        "suggestion": ""
    }
    
    result = workflow.invoke(initial_state)
    return {
        "analysis": result["analysis"],
        "suggestion": result["suggestion"]
    }

if __name__ == "__main__":
    # Example usage
    sample_error = """
    Traceback (most recent call last):
      File "example.py", line 10, in <module>
        result = divide_numbers(5, 0)
    ZeroDivisionError: division by zero
    """
    
    result = process_error(sample_error)
    print("Analysis:", result["analysis"])
    print("Suggestion:", result["suggestion"])
