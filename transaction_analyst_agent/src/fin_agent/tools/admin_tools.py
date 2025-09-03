from src.fin_agent.common.bq_client import get_bq_toolset

def reset_all_transactions() -> str:
    """
    Resets all processing-derived fields in the transactions table back to NULL.
    This is a destructive operation that clears all cleanup and categorization work.
    """
    bq_toolset = get_bq_toolset()
    query = """
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        merchant_name_cleaned = NULL,
        description_cleaned = NULL,
        primary_category = NULL,
        secondary_category = NULL,
        transaction_type = NULL,
        category_method = NULL,
        rule_id = NULL
    WHERE true; -- This ensures all rows in the table are updated.
    """
    try:
        bq_toolset.execute_query(query)
        # It's good practice to confirm which table was reset in the message.
        return "Successfully reset all derived fields in the `transactions` table."
    except Exception as e:
        return f"An error occurred while resetting transaction data: {e}"
