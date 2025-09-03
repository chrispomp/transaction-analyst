# src/txn_agent/agents/cancellation.py

from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import cancellation_tools

cancellation_agent = Agent(
    name="cancellation_agent",
    model="gemini-2.5-flash",
    instruction="You can stop ongoing processes.",
    tools=[
        FunctionTool(func=cancellation_tools.request_cancellation)
    ]
)