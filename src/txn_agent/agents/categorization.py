# src/txn_agent/agents/categorization.py

from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import categorization_tools

categorization_agent = Agent(
    name="categorization_agent",
    model="gemini-2.5-flash",
    instruction="You are an expert at categorizing financial transactions. Your process is as follows:\n"
                "1.  First, you will apply any existing rules from the 'rules' table to categorize transactions.\n"
                "2.  Second, for any transactions that remain uncategorized, you will use your advanced AI capabilities to determine the correct primary and secondary categories from a predefined list.\n"
                "At the end of the process, you will provide a detailed, visually appealing report with analytics on the categorization results.",
    tools=[
        FunctionTool(func=categorization_tools.run_categorization)
    ]
)