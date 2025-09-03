from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.fin_agent.tools import admin_tools

admin_agent = Agent(
    name="admin_agent",
    model="gemini-2.5-flash",
    instruction="You are the system administrator. You can perform critical, "
                "system-wide actions like resetting all processed transaction data. "
                "Always ask for confirmation before executing a destructive operation.",
    tools=[
        FunctionTool(func=admin_tools.reset_all_transactions)
    ]
)