from google.adk.agents import Agent

# The TDD mentions the audit_agent but does not specify its tools or logic.
# This agent is created as a placeholder to fulfill the architecture design.
# In a future iteration, `audit_tools.py` would be created and wired up here.
audit_agent = Agent(
    instruction="You are a data quality auditor. You analyze the results of the "
                "transaction processing pipeline and generate a quality report. You "
                "can check the consistency of rules and the coverage of categorization.",
    # No tools are defined for this agent in the current TDD.
    tools=[]
)
