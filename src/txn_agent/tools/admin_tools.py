from google.cloud import bigquery
from typing import Literal
from datetime import datetime, timedelta

def reset_all_transactions(timeframe: Literal["last 3 months", "last 6 months", "all transactions"], confirmation: Literal["CONFIRM"] | None = None) -> str:
    """
    Resets all processing-derived fields in the transactions table back to NULL for a specified timeframe.
    This is a destructive operation that requires explicit confirmation.
    To proceed, you must pass the exact string "CONFIRM" to this tool.
    """
    if confirmation != "CONFIRM":
        return ('🤔 **Confirmation Needed**: To reset all processed transaction data, '
                'please explicitly state "Reset all processed transaction data" or confirm it by '
                'typing `CONFIRM`.')

    client = bigquery.Client()
    
    end_date = datetime.utcnow()
    if timeframe == "last 3 months":
        start_date = end_date - timedelta(days=90)
        where_clause = f"WHERE transaction_date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"
    elif timeframe == "last 6 months":
        start_date = end_date - timedelta(days=180)
        where_clause = f"WHERE transaction_date BETWEEN '{start_date.isoformat()}' AND '{end_date.isoformat()}'"
    else: # all transactions
        where_clause = "WHERE true"


    query = f"""
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        merchant_name_cleaned = NULL,
        description_cleaned = NULL,
        primary_category = NULL,
        secondary_category = NULL,
        transaction_type = NULL,
        categorization_method = NULL,
        rule_id = NULL
    {where_clause};
    """
    try:
        job = client.query(query)
        job.result()
        return f"✅ **Success!** All derived fields in the `transactions` table have been reset for {timeframe}. {job.num_dml_affected_rows} rows affected."
    except Exception as e:
        return f"🚨 **Error**: An error occurred while resetting transaction data: {e}"