from google.cloud import bigquery
from typing import Literal

def reset_all_transactions(confirmation: Literal["CONFIRM"] | None = None) -> str:
    """
    Resets all processing-derived fields in the transactions table back to NULL.
    This is a destructive operation that requires explicit confirmation.
    To proceed, you must pass the exact string "CONFIRM" to this tool.
    """
    if confirmation != "CONFIRM":
        return ('🤔 **Confirmation Needed**: To reset all processed transaction data, '
                'please explicitly state "Reset all processed transaction data" or confirm it by '
                'typing `CONFIRM`.')

    client = bigquery.Client()
    query = """
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        merchant_name_cleaned = NULL,
        description_cleaned = NULL,
        primary_category = NULL,
        secondary_category = NULL,
        transaction_type = NULL,
        categorization_method = NULL,
        rule_id = NULL
    WHERE true; -- This ensures all rows in the table are updated.
    """
    try:
        client.query(query).result()
        return "✅ **Success!** All derived fields in the `transactions` table have been reset."
    except Exception as e:
        return f"🚨 **Error**: An error occurred while resetting transaction data: {e}"