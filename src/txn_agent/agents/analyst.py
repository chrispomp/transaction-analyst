from google.adk.agents import Agent
from src.txn_agent.common.bq_client import get_bq_toolset

transaction_analyst = Agent(
    name="transaction_analyst",
    instruction="You are an expert financial data analyst. Your goal is to answer "
                "the user's questions by converting their natural language into an "
                "efficient, read-only BigQuery SQL query. Execute the query and "
                "present the answer clearly. Never modify data.",
    tools=get_bq_toolset(read_only=True)
)