from google.cloud import bigquery
from src.txn_agent.common.constants import VALID_CATEGORIES
import pandas as pd

def create_rule(primary_category: str, secondary_category: str, merchant_match: str, persona: str = 'general', confidence: float = 0.99) -> str:
    """
    Creates a new categorization rule in the 'rules' table.
    Inputs are parameterized to prevent SQL injection.
    """
    if primary_category not in VALID_CATEGORIES or secondary_category not in VALID_CATEGORIES.get(primary_category, []):
        return f"⚠️ Invalid category specified. Please choose from the available categories."

    client = bigquery.Client()
    query = """
    INSERT INTO `fsi-banking-agentspace.txns.rules`
        (primary_category, secondary_category, merchant_name_cleaned_match, persona_type, confidence_score, status)
    VALUES (@primary_category, @secondary_category, @merchant_match, @persona, @confidence, 'active')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("primary_category", "STRING", primary_category),
            bigquery.ScalarQueryParameter("secondary_category", "STRING", secondary_category),
            bigquery.ScalarQueryParameter("merchant_match", "STRING", merchant_match),
            bigquery.ScalarQueryParameter("persona", "STRING", persona),
            bigquery.ScalarQueryParameter("confidence", "FLOAT", confidence),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return f"✅ Successfully created a new rule for '{merchant_match}'."
    except Exception as e:
        return f"🚨 Error creating rule: {e}"

def update_rule_status(rule_id: int, status: str) -> str:
    """Updates the status of a rule (e.g., 'active', 'inactive')."""
    if status not in ['active', 'inactive']:
        return "⚠️ Invalid status. Must be 'active' or 'inactive'."

    client = bigquery.Client()
    query = """
    UPDATE `fsi-banking-agentspace.txns.rules`
    SET status = @status
    WHERE rule_id = @rule_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("rule_id", "INT64", rule_id),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        return f"✅ Successfully updated rule {rule_id} to '{status}'."
    except Exception as e:
        return f"🚨 Error updating rule: {e}"

def suggest_new_rules() -> str:
    """
    Analyzes LLM-categorized transactions to suggest new, high-confidence rules
    for merchants that frequently get categorized by the LLM.
    """
    client = bigquery.Client()
    query = """
    SELECT
        merchant_name_cleaned,
        primary_category,
        secondary_category,
        COUNT(*) AS transaction_count
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE categorization_method = 'llm-powered'
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 5
    ORDER BY transaction_count DESC
    LIMIT 10;
    """
    try:
        suggestions_df = client.query(query).to_dataframe()
        if suggestions_df.empty:
            return "👍 No new rule suggestions found at this time."

        suggestions_str = "Here are some new rule suggestions for your approval:\n\n"
        suggestions_str += "| Merchant | Suggested Category | Based On |\n"
        suggestions_str += "|---|---|---|\n"
        for _, row in suggestions_df.iterrows():
            suggestions_str += (
                f"| {row['merchant_name_cleaned']} | "
                f"{row['primary_category']} / {row['secondary_category']} | "
                f"{row['transaction_count']} recent transactions |\n"
            )

        return suggestions_str
    except Exception as e:
        return f"🚨 Error generating new rule suggestions: {e}"