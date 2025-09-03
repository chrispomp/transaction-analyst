from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import rules_manager_tools

rules_manager = Agent(
    instruction="You manage categorization rules. You can create, update, and "
                "deactivate rules. You can also analyze transaction data to "
                "suggest new, high-confidence rules for the user to approve.",
    tools=[
        FunctionTool(
            func=rules_manager_tools.create_rule,
            description="Creates a new categorization rule based on user-provided details."
        ),
        FunctionTool(
            func=rules_manager_tools.update_rule_status,
            description="Updates the status of an existing rule (e.g., to 'active' or 'inactive')."
        ),
        FunctionTool(
            func=rules_manager_tools.suggest_new_rules,
            description="Analyzes recently LLM-categorized data to suggest new, high-quality rules."
        )
    ]
)