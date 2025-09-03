from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import admin_tools

admin_agent = Agent(
    instruction="You are the system administrator. You can perform critical, "
                "system-wide actions like resetting all processed transaction data. "
                "Always ask for confirmation before executing a destructive operation.",
    tools=[
        FunctionTool(
            func=admin_tools.reset_all_transactions,
            description="Resets all derived/processed fields in the transactions "
                        "table to NULL. This is a destructive action and should "
                        "be confirmed with the user."
        )
    ]
)