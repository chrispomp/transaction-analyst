from google.adk.agents import Agent, FunctionTool
from src.fin_agent.tools import categorization_tools

categorization_agent = Agent(
    instruction="You categorize financial transactions in two stages. First, apply all "
                "matching rules from the rules table. Second, for any remaining "
                "uncategorized transactions, use your LLM intelligence to determine "
                "the correct categories.",
    tools=[
        FunctionTool(
            func=categorization_tools.run_categorization,
            description="Runs the hybrid rule-based and LLM-powered categorization process."
        )
    ]
)
