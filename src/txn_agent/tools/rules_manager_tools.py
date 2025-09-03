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
_rule_suggestions_cache = None

def create_rule(primary_category: str, secondary_category: str, identifier: str, identifier_type: str, transaction_type: str, persona: str = 'general', confidence: float = 0.99) -> str:
    """
    Creates a new categorization rule in the 'rules' table.
    Inputs are parameterized to prevent SQL injection.
    """
    logger.info(f"Attempting to create a new rule for identifier: '{identifier}'")
    client = bigquery.Client()

    # Validation checks
    if primary_category not in VALID_CATEGORIES or secondary_category not in VALID_CATEGORIES.get(primary_category, []):
        logger.warning(f"Invalid category specified: {primary_category}/{secondary_category}")
        return f"⚠️ **Invalid Category**: `{primary_category}/{secondary_category}` is not a valid category combination. Please choose from the available categories."

    if transaction_type not in ['DEBIT', 'Credit']:
        logger.warning(f"Invalid transaction type: {transaction_type}")
        return f"⚠️ **Invalid Transaction Type**: `{transaction_type}` is not a valid transaction type. It must be either 'DEBIT' or 'Credit'."

    if identifier_type not in ['merchant_name_cleaned', 'description_cleaned']:
        logger.warning(f"Invalid identifier_type: {identifier_type}")
        return f"⚠️ **Invalid Identifier Type**: `{identifier_type}` is not a valid identifier type. It must be either 'merchant_name_cleaned' or 'description_cleaned'."

    # Check for existing or conflicting rules
    check_query = """
    SELECT rule_id, primary_category, secondary_category
    FROM `fsi-banking-agentspace.txns.rules`
    WHERE @identifier LIKE '%' || identifier || '%'
      AND identifier_type = @identifier_type
      AND transaction_type = @transaction_type
    """
    job_config_check = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("identifier", "STRING", identifier),
            bigquery.ScalarQueryParameter("identifier_type", "STRING", identifier_type),
            bigquery.ScalarQueryParameter("transaction_type", "STRING", transaction_type),
        ]
    )
    
    try:
        existing_rules = client.query(check_query, job_config=job_config_check).to_dataframe()
        if not existing_rules.empty:
            for _, rule in existing_rules.iterrows():
                if rule['primary_category'] == primary_category and rule['secondary_category'] == secondary_category:
                    return f"✅ **Rule Already Exists**: A rule with this exact configuration already exists (Rule ID: `{rule['rule_id']}`). No action was taken."
                else:
                    return (f"⚠️ **Conflicting Rule Alert**: A conflicting rule already exists (Rule ID: `{rule['rule_id']}`) "
                            f"that categorizes '{identifier}' as `{rule['primary_category']} / {rule['secondary_category']}`. "
                            f"Please resolve this conflict before creating a new rule.")
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error during rule check: {e}")
        return f"🚨 **Error**: A database error occurred while checking for existing rules: {e}"

    # If no conflicts, create the new rule
    rule_id = str(uuid.uuid4())
    insert_query = """
    INSERT INTO `fsi-banking-agentspace.txns.rules`
        (rule_id, primary_category, secondary_category, identifier, identifier_type, transaction_type, persona_type, confidence_score, status)
    VALUES (@rule_id, @primary_category, @secondary_category, @identifier, @identifier_type, @transaction_type, @persona, @confidence, 'active')
    """
    job_config_insert = bigquery.QueryJobConfig(
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
        client.query(insert_query, job_config=job_config_insert).result()
        logger.info(f"Successfully created rule for '{identifier}' with rule_id: {rule_id}.")
        return f"✅ **Rule Created!** A new rule for `{identifier}` has been successfully created with Rule ID: `{rule_id}`."
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error creating rule: {e}")
        return f"🚨 **Error**: A database error occurred while creating the new rule: {e}"

def update_rule_status(rule_id: int, status: str) -> str:
    """Updates the status of a rule (e.g., 'active', 'inactive')."""
    logger.info(f"Attempting to update rule ID {rule_id} to status '{status}'")
    if status not in ['active', 'inactive']:
        logger.warning(f"Invalid status '{status}' provided for rule update.")
        return "⚠️ **Invalid Status**: The status must be either 'active' or 'inactive'."

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
        return f"✅ **Rule Updated!** The status of Rule ID `{rule_id}` has been successfully updated to `{status}`."
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error updating rule {rule_id}: {e}")
        return f"🚨 **Error**: A database error occurred while updating the rule: {e}"

def suggest_new_rules() -> str:
    """
    Analyzes LLM-categorized transactions to suggest new, high-confidence rules
    for merchants that frequently get categorized by the LLM.
    """
    global _rule_suggestions_cache
    logger.info("Analyzing LLM-categorized transactions for new rule suggestions.")
    client = bigquery.Client()
    query = """
    WITH PotentialRules AS (
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
        HAVING COUNT(*) > 1
    )
    SELECT
        pr.identifier,
        pr.identifier_type,
        pr.primary_category,
        pr.secondary_category,
        pr.transaction_type,
        pr.transaction_count
    FROM PotentialRules pr
    WHERE NOT EXISTS (
        SELECT 1
        FROM `fsi-banking-agentspace.txns.rules` r
        WHERE pr.identifier LIKE '%' || r.identifier || '%'
        AND pr.identifier_type = r.identifier_type
        AND pr.transaction_type = r.transaction_type
    )
    ORDER BY pr.transaction_count DESC
    LIMIT 10;
    """
    try:
        suggestions_df = client.query(query).to_dataframe()
        _rule_suggestions_cache = suggestions_df.to_dict(orient='records')

        if suggestions_df.empty:
            logger.info("No new rule suggestions found.")
            return "👍 **No New Suggestions**: I couldn't find any new rule suggestions at this time."

        suggestions_str = "💡 **Here are some new rule suggestions for your approval:**\n\n"
        suggestions_str += "| Identifier | Identifier Type | Suggested Category | Transaction Type | Based On |\n"
        suggestions_str += "|---|---|---|---|---|\n"
        for _, row in suggestions_df.iterrows():
            suggestions_str += (
                f"| `{row['identifier']}` | `{row['identifier_type']}` | `{row['primary_category']} / {row['secondary_category']}` | `{row['transaction_type']}` | `{row['transaction_count']} recent transactions` |\n"
            )

        logger.info(f"Generated {len(suggestions_df)} new rule suggestions.")
        return suggestions_str
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error generating new rule suggestions: {e}")
        return f"🚨 **Error**: A database error occurred while generating new rule suggestions: {e}"

def bulk_create_rules() -> str:
    """
    Creates all rules from the last set of suggestions.
    """
    global _rule_suggestions_cache
    if not _rule_suggestions_cache:
        return "⚠️ **No Suggestions to Approve**: Please run `suggest_new_rules` first to generate a list of suggestions."

    results = []
    for suggestion in _rule_suggestions_cache:
        result = create_rule(
            primary_category=suggestion['primary_category'],
            secondary_category=suggestion['secondary_category'],
            identifier=suggestion['identifier'],
            identifier_type=suggestion['identifier_type'],
            transaction_type=suggestion['transaction_type']
        )
        results.append(result)
    
    # Clear the cache after processing
    _rule_suggestions_cache = None
    
    return "✅ **Bulk Rule Creation Complete!**\n\n" + "\n".join(results)