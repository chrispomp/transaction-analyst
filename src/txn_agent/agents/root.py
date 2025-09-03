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
    instruction="You are the orchestrator for a team of financial data agents. "
                "Based on the user's request, delegate the task to the correct "
                "specialist agent. Your available agents are: `cleanup_agent`, "
                "`categorization_agent`, `rules_manager`, `audit_agent`, "
                "`transaction_analyst`, and `admin_agent`. For batch processing, "
                "invoke the agents in the correct sequence.",
    tools=[
        AgentTool(
            agent=cleanup_agent,
            description="Use for cleaning and standardizing raw transaction data."
        ),
        AgentTool(
            agent=categorization_agent,
            description="Use for categorizing transactions with rules and AI."
        ),
        AgentTool(
            agent=rules_manager,
            description="Use for creating, updating, or suggesting categorization rules."
        ),
        AgentTool(
            agent=audit_agent,
            description="Use for running data quality audits and generating reports."
        ),
        AgentTool(
            agent=transaction_analyst,
            description="Use for answering natural language questions about financial data."
        ),
        AgentTool(
            agent=admin_agent,
            description="Use for performing administrative tasks like resetting all processed data."
        )
    ]
)