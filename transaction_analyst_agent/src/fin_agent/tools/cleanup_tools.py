from src.fin_agent.common.bq_client import get_bq_toolset

def run_full_cleanup() -> str:
    """
    Cleans and standardizes raw transaction data in BigQuery.
    - Standardizes merchant names and descriptions.
    - Corrects transaction types based on the sign of the amount.
    """
    bq_toolset = get_bq_toolset()

    # This query standardizes text fields by converting them to uppercase,
    # trimming whitespace, and removing special characters.
    standardize_query = """
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        merchant_name_cleaned = UPPER(TRIM(REGEXP_REPLACE(merchant_name, r'[^A-Z0-9\\s]', ''))),
        description_cleaned = UPPER(TRIM(REGEXP_REPLACE(description, r'[^A-Z0-9\\s]', '')))
    WHERE merchant_name_cleaned IS NULL OR description_cleaned IS NULL;
    """

    # This query corrects the transaction_type based on the transaction amount.
    correct_type_query = """
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        transaction_type = CASE
            WHEN amount < 0 THEN 'DEBIT'
            WHEN amount > 0 THEN 'CREDIT'
            ELSE 'ZERO'
        END
    WHERE transaction_type IS NULL OR
          (amount < 0 AND transaction_type != 'DEBIT') OR
          (amount > 0 AND transaction_type != 'CREDIT');
    """

    try:
        # Note: The ADK's BigQueryToolset.execute_query does not return the
        # number of rows modified by DML statements. A more advanced implementation
        # might use the google-cloud-bigquery client library directly to get
        # job statistics for a more detailed summary.
        bq_toolset.execute_query(query=standardize_query)
        bq_toolset.execute_query(query=correct_type_query)

        return "Cleanup successful. Text fields were standardized and transaction types were corrected."
    except Exception as e:
        # In a production system, log the full error.
        return f"An error occurred during data cleanup: {e}"
