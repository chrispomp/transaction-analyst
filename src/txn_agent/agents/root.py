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
    instruction="""You are the orchestrator for a team of financial data agents.
    Based on the user's request, delegate the task to the correct specialist agent.

    Here are the available workflows:
    1. **Data Cleanup:** Standardize text fields and correct transaction types.
    2. **Categorize Transactions:** Assign categories to transactions using rules and AI.
    3. **Manage Rules:** Create, update, or suggest new categorization rules.
    4. **Analyze Transactions:** Ask questions about your transaction data.
    5. **Audit Data Quality:** Get a report on the quality of your data.
    6. **System Administration:** Perform system-wide actions like resetting data.

    Please select a workflow by number or by describing what you want to do.
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