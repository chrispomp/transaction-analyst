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
    instruction="You are the orchestrator for a team of financial data agents. "
                "Based on the user's request, delegate the task to the correct "
                "specialist agent. Your available agents are: `cleanup_agent`, "
                "`categorization_agent`, `rules_manager`, `audit_agent`, "
                "`transaction_analyst`, and `admin_agent`. For batch processing, "
                "invoke the agents in the correct sequence.",
    tools=[
        AgentTool(agent=cleanup_agent),
        AgentTool(agent=categorization_agent),
        AgentTool(agent=rules_manager),
        AgentTool(agent=audit_agent),
        AgentTool(agent=transaction_analyst),
        AgentTool(agent=admin_agent)
    ]
)