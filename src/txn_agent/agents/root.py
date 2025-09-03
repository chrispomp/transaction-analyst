# src/txn_agent/agents/root.py

from google.adk.agents import Agent
from google.adk.tools import AgentTool

# Import all the specialized sub-agent instances that the root agent will orchestrate.
from .cleanup import cleanup_agent
from .categorization import categorization_agent
from .rules_manager import rules_manager
from .audit import audit_agent
from .analyst import transaction_analyst
from .admin import admin_agent

root_agent = Agent(
    name="root_agent",
    model="gemini-2.5-flash",
    instruction="""
    Welcome to your financial data assistant! 📈 I can help you with a variety of tasks.

    Please select one of the following workflows:

    1.  🧹 **Data Cleanup**: Standardize text fields and correct transaction types.
    2.  🗂️ **Categorize Transactions**: Assign categories to transactions using rules and AI.
    3.  룰 **Manage Rules**: Create, update, or suggest new categorization rules.
    4.  📊 **Analyze Transactions**: Ask questions about your transaction data.
    5.   auditing **Audit Data Quality**: Get a report on the quality of your data.
    6.  ⚙️ **System Administration**: Perform system-wide actions like resetting data.

    You can select a workflow by number or by describing what you want to do.
    """,
    tools=[
        AgentTool(agent=cleanup_agent),
        AgentTool(agent=categorization_agent),
        AgentTool(agent=rules_manager),
        AgentTool(agent=audit_agent),
        AgentTool(agent=transaction_analyst),
        AgentTool(agent=admin_agent)
    ]
)