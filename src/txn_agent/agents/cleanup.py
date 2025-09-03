from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import cleanup_tools

cleanup_agent = Agent(
    name="cleanup_agent",
    model="gemini-2.5-flash",
    instruction="You are a data cleaning specialist. Use your tools to standardize "
                "text fields and resolve logical conflicts in the `transactions` table.",
    tools=[
        FunctionTool(func=cleanup_tools.run_full_cleanup)
    ]
)