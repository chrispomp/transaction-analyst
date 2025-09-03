from src.txn_agent.common.bq_client import get_bq_toolset
from typing import Literal

def reset_all_transactions(confirmation: Literal["CONFIRM reset"] | None = None) -> str:
    """
    Resets all processing-derived fields in the transactions table back to NULL.
    This is a destructive operation that requires explicit confirmation.
    To proceed, you must pass the exact string "CONFIRM reset" to this tool.
    """
    if confirmation != "CONFIRM reset":
        return ('It looks like I need more context about what you\'re trying to '
                'confirm. To reset all processed transaction data, please explicitly '
                'state "Reset all processed transaction data" or confirm it by '
                'typing "CONFIRM reset".')

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
        bq_toolset.execute_sql(query)
        # It's good practice to confirm which table was reset in the message.
        return "Successfully reset all derived fields in the `transactions` table."
    except Exception as e:
        return f"An error occurred while resetting transaction data: {e}"