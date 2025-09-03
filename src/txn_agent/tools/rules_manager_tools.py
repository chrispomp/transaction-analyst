from src.txn_agent.common.bq_client import get_bq_toolset

def create_rule(primary_category: str, secondary_category: str, merchant_match: str, persona: str = 'general', confidence: float = 0.99) -> str:
    """
    Creates a new categorization rule in the 'rules' table.
    Inputs are parameterized to prevent SQL injection.
    """
    bq_toolset = get_bq_toolset()
    # Using parameterized queries is a security best practice.
    # The ADK BigQueryToolset supports this via the query_parameters argument.
    query = """
    INSERT INTO `fsi-banking-agentspace.txns.rules`
        (primary_category, secondary_category, merchant_name_cleaned_match, persona_type, confidence_score, status)
    VALUES (@primary_category, @secondary_category, @merchant_match, @persona, @confidence, 'active')
    """
    params = {
        "primary_category": primary_category,
        "secondary_category": secondary_category,
        "merchant_match": merchant_match,
        "persona": persona,
        "confidence": confidence
    }
    try:
        bq_toolset.execute_query(query, query_parameters=params)
        return "Successfully created new rule."
    except Exception as e:
        return f"Error creating rule: {e}"

def update_rule_status(rule_id: int, status: str) -> str:
    """Updates the status of a rule (e.g., 'active', 'inactive')."""
    if status not in ['active', 'inactive']:
        return "Invalid status. Must be 'active' or 'inactive'."

    bq_toolset = get_bq_toolset()
    query = """
    UPDATE `fsi-banking-agentspace.txns.rules`
    SET status = @status
    WHERE rule_id = @rule_id
    """
    params = {"status": status, "rule_id": rule_id}
    try:
        bq_toolset.execute_query(query, query_parameters=params)
        return f"Successfully updated rule {rule_id} to {status}."
    except Exception as e:
        return f"Error updating rule: {e}"

def suggest_new_rules() -> str:
    """
    Analyzes LLM-categorized transactions to suggest new, high-confidence rules
    for merchants that frequently get categorized by the LLM.
    """
    bq_toolset = get_bq_toolset()
    query = """
    SELECT
        merchant_name_cleaned,
        primary_category,
        secondary_category,
        COUNT(*) AS transaction_count
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE category_method = 'llm-powered'
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 5 -- Suggest rules for merchants appearing frequently
    ORDER BY transaction_count DESC
    LIMIT 10;
    """
    try:
        suggestions_df = bq_toolset.execute_query(query)
        if suggestions_df.empty:
            return "No new rule suggestions found at this time."

        suggestions_str = "Suggested New Rules (for user approval):\n"
        for _, row in suggestions_df.iterrows():
            suggestions_str += (
                f"- For merchant '{row['merchant_name_cleaned']}', "
                f"suggest category: {row['primary_category']} / {row['secondary_category']} "
                f"(based on {row['transaction_count']} recent transactions).\n"
            )

        return suggestions_str
    except Exception as e:
        return f"Error generating new rule suggestions: {e}"
