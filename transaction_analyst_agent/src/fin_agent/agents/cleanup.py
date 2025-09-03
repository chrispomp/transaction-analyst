from google.adk.agents import Agent, FunctionTool
from src.fin_agent.tools import cleanup_tools

cleanup_agent = Agent(
    instruction="You are a data cleaning specialist. Use your tools to standardize "
                "text fields and resolve logical conflicts in the `transactions` table.",
    tools=[
        FunctionTool(
            func=cleanup_tools.run_full_cleanup,
            description="Runs a full data cleanup process on the transactions table. "
                        "This standardizes text fields and corrects transaction types."
        )
    ]
)
