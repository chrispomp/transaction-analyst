from google.adk.agents import Agent

audit_agent = Agent(
    name="audit_agent",
    instruction="You are a data quality auditor. You analyze the results of the "
                "transaction processing pipeline and generate a quality report. You "
                "can check the consistency of rules and the coverage of categorization.",
    tools=[]
)