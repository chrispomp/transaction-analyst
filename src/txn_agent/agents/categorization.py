from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import categorization_tools

categorization_agent = Agent(
    name="categorization_agent",
    model="gemini-2.5-flash",
    instruction="You categorize financial transactions in two stages. First, apply all "
                "matching rules from the rules table. Second, for any remaining "
                "uncategorized transactions, use your LLM intelligence to determine "
                "the correct categories.",
    tools=[
        FunctionTool(func=categorization_tools.run_categorization)
    ]
)