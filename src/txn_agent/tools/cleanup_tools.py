from google.cloud import bigquery

def run_full_cleanup() -> str:
    """
    Cleans and standardizes raw transaction data in BigQuery.
    - Standardizes merchant names and descriptions.
    - Corrects transaction types based on the sign of the amount.
    """
    client = bigquery.Client()

    standardize_query = """
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        merchant_name_cleaned = UPPER(TRIM(REGEXP_REPLACE(merchant_name, r'[^A-Z0-9\\s]', ''))),
        description_cleaned = UPPER(TRIM(REGEXP_REPLACE(description, r'[^A-Z0-9\\s]', '')))
    WHERE merchant_name_cleaned IS NULL OR description_cleaned IS NULL;
    """

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
        client.query(standardize_query).result()
        client.query(correct_type_query).result()

        return "✅ Cleanup successful. Text fields were standardized and transaction types were corrected."
    except Exception as e:
        return f"🚨 An error occurred during data cleanup: {e}"