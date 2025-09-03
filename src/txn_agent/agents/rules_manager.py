# src/txn_agent/agents/rules_manager.py

from google.adk.agents import Agent
from google.adk.tools import FunctionTool
# This import is now pointing to the refactored tools file
from src.txn_agent.tools import rules_manager_tools 

rules_manager = Agent(
    name="rules_manager",
    model="gemini-2.5-flash",
    instruction="I'm here to help you manage your categorization rules. You can ask me to create, update, or deactivate rules. "
                "I can also analyze your transaction data to suggest new, high-confidence rules for you to approve. "
                "When a user approves a suggestion, use the 'create_rule' tool to create the rule with the parameters from the suggestion.",
    tools=[
        FunctionTool(func=rules_manager_tools.create_rule),
        FunctionTool(func=rules_manager_tools.update_rule_status),
        FunctionTool(func=rules_manager_tools.suggest_new_rules)
    ]
)