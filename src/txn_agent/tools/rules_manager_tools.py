# src/txn_agent/tools/rules_manager_tools.py

from __future__ import annotations
import logging
import uuid
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from src.txn_agent.common.constants import VALID_CATEGORIES
import pandas as pd

# Set up a logger for this module
logger = logging.getLogger(__name__)

def create_rule(primary_category: str, secondary_category: str, identifier: str, identifier_type: str, transaction_type: str, persona: str = 'general', confidence: float = 0.99) -> str:
    """
    Creates a new categorization rule in the 'rules' table.
    Inputs are parameterized to prevent SQL injection.
    """
    logger.info(f"Attempting to create a new rule for identifier: '{identifier}'")
    if primary_category not in VALID_CATEGORIES or secondary_category not in VALID_CATEGORIES.get(primary_category, []):
        logger.warning(f"Invalid category specified: {primary_category}/{secondary_category}")
        return f"⚠️ Invalid category specified. Please choose from the available categories."

    if transaction_type not in ['DEBIT', 'CREDIT']:
        logger.warning(f"Invalid transaction type: {transaction_type}")
        return f"⚠️ Invalid transaction type specified. Must be 'DEBIT' or 'CREDIT'."

    if identifier_type not in ['merchant_name_cleaned', 'description_cleaned']:
        logger.warning(f"Invalid identifier_type: {identifier_type}")
        return f"⚠️ Invalid identifier_type specified. Must be 'merchant_name_cleaned' or 'description_cleaned'."

    client = bigquery.Client()
    rule_id = str(uuid.uuid4())

    query = """
    INSERT INTO `fsi-banking-agentspace.txns.rules`
        (rule_id, primary_category, secondary_category, identifier, identifier_type, transaction_type, persona_type, confidence_score, status)
    VALUES (@rule_id, @primary_category, @secondary_category, @identifier, @identifier_type, @transaction_type, @persona, @confidence, 'active')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("rule_id", "STRING", rule_id),
            bigquery.ScalarQueryParameter("primary_category", "STRING", primary_category),
            bigquery.ScalarQueryParameter("secondary_category", "STRING", secondary_category),
            bigquery.ScalarQueryParameter("identifier", "STRING", identifier),
            bigquery.ScalarQueryParameter("identifier_type", "STRING", identifier_type),
            bigquery.ScalarQueryParameter("transaction_type", "STRING", transaction_type),
            bigquery.ScalarQueryParameter("persona", "STRING", persona),
            bigquery.ScalarQueryParameter("confidence", "FLOAT", confidence),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
        logger.info(f"Successfully created rule for '{identifier}' with rule_id: {rule_id}.")
        return f"✅ Successfully created a new rule for '{identifier}'."
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error creating rule: {e}")
        return f"🚨 Error creating rule: {e}"

def update_rule_status(rule_id: int, status: str) -> str:
    """Updates the status of a rule (e.g., 'active', 'inactive')."""
    logger.info(f"Attempting to update rule ID {rule_id} to status '{status}'")
    if status not in ['active', 'inactive']:
        logger.warning(f"Invalid status '{status}' provided for rule update.")
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
        logger.info(f"Successfully updated rule {rule_id}.")
        return f"✅ Successfully updated rule {rule_id} to '{status}'."
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error updating rule {rule_id}: {e}")
        return f"🚨 Error updating rule: {e}"

def suggest_new_rules() -> str:
    """
    Analyzes LLM-categorized transactions to suggest new, high-confidence rules
    for merchants that frequently get categorized by the LLM.
    """
    logger.info("Analyzing LLM-categorized transactions for new rule suggestions.")
    client = bigquery.Client()
    query = """
    SELECT
        identifier,
        identifier_type,
        primary_category,
        secondary_category,
        transaction_type,
        COUNT(*) AS transaction_count
    FROM (
        SELECT
            merchant_name_cleaned as identifier,
            'merchant_name_cleaned' as identifier_type,
            primary_category,
            secondary_category,
            transaction_type
        FROM `fsi-banking-agentspace.txns.transactions`
        WHERE categorization_method = 'llm-powered'
        UNION ALL
        SELECT
            description_cleaned as identifier,
            'description_cleaned' as identifier_type,
            primary_category,
            secondary_category,
            transaction_type
        FROM `fsi-banking-agentspace.txns.transactions`
        WHERE categorization_method = 'llm-powered'
    )
    GROUP BY 1, 2, 3, 4, 5
    HAVING COUNT(*) > 2
    ORDER BY transaction_count DESC
    LIMIT 10;
    """
    try:
        suggestions_df = client.query(query).to_dataframe()
        if suggestions_df.empty:
            logger.info("No new rule suggestions found.")
            return "👍 No new rule suggestions found at this time."

        suggestions_str = "Here are some new rule suggestions for your approval:\n\n"
        suggestions_str += "| Identifier | Identifier Type | Suggested Category | Transaction Type | Based On |\n"
        suggestions_str += "|---|---|---|---|---|\n"
        for _, row in suggestions_df.iterrows():
            suggestions_str += (
                f"| {row['identifier']} | "
                f"{row['identifier_type']} | "
                f"{row['primary_category']} / {row['secondary_category']} | "
                f"{row['transaction_type']} | "
                f"{row['transaction_count']} recent transactions |\n"
            )

        logger.info(f"Generated {len(suggestions_df)} new rule suggestions.")
        return suggestions_str
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error generating new rule suggestions: {e}")
        return f"🚨 Error generating new rule suggestions: {e}"