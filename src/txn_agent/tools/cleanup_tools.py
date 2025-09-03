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
        merchant_name_cleaned = TRIM(REGEXP_REPLACE(UPPER(merchant_name_raw), r'[^A-Z0-9]+', ' ')),
        description_cleaned = TRIM(REGEXP_REPLACE(UPPER(description_raw), r'[^A-Z0-9]+', ' '))
    WHERE merchant_name_cleaned IS NULL OR description_cleaned IS NULL;
    """

    correct_type_query = """
    UPDATE `fsi-banking-agentspace.txns.transactions`
    SET
        transaction_type = CASE
            WHEN amount < 0 THEN 'Debit'
            WHEN amount > 0 THEN 'Credit'
            ELSE 'ZERO'
        END
    WHERE transaction_type IS NULL OR
          (amount < 0 AND transaction_type != 'Debit') OR
          (amount > 0 AND transaction_type != 'Credit');
    """

    try:
        client.query(standardize_query).result()
        client.query(correct_type_query).result()

        return "✅ **Cleanup Successful!** Text fields were standardized and transaction types were corrected."
    except Exception as e:
        return f"🚨 **An error occurred during data cleanup:** {e}"