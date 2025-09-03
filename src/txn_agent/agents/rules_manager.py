from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from src.txn_agent.tools import rules_manager_tools

rules_manager = Agent(
    name="rules_manager",
    instruction="You manage categorization rules. You can create, update, and "
                "deactivate rules. You can also analyze transaction data to "
                "suggest new, high-confidence rules for the user to approve.",
    tools=[
        FunctionTool(func=rules_manager_tools.create_rule),
        FunctionTool(func=rules_manager_tools.update_rule_status),
        FunctionTool(func=rules_manager_tools.suggest_new_rules)
    ]
)