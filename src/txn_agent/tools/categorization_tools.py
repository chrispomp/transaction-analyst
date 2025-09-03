# src/txn_agent/tools/categorization_tools.py

from __future__ import annotations
import json
import logging
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from vertexai.generative_models import GenerativeModel
from src.txn_agent.common.constants import VALID_CATEGORIES

# Set up a logger for this module
logger = logging.getLogger(__name__)

def run_categorization() -> str:
    """
    Categorizes transactions using a hybrid rules-based and LLM-powered approach.
    """
    logger.info("Starting categorization process...")
    client = bigquery.Client()

    # Stage 1: Apply existing rules using a MERGE statement.
    logger.info("Stage 1: Applying rules-based categorization.")
    rules_merge_query = """
    MERGE `fsi-banking-agentspace.txns.transactions` AS T
    USING (
        SELECT rule_id, primary_category, secondary_category, merchant_name_cleaned_match
        FROM `fsi-banking-agentspace.txns.rules`
        WHERE status = 'active'
    ) AS R
    ON T.merchant_name_cleaned = R.merchant_name_cleaned_match
    WHEN MATCHED AND T.primary_category IS NULL THEN
        UPDATE SET
            primary_category = R.primary_category,
            secondary_category = R.secondary_category,
            categorization_method = 'rule-based',
            rule_id = R.rule_id
    """
    try:
        merge_job = client.query(rules_merge_query)
        merge_job.result() # Wait for the job to complete
        logger.info(f"Rules-based categorization affected {merge_job.num_dml_affected_rows} rows.")
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error during rule-based categorization: {e}")
        return f"🚨 An error occurred during rule-based categorization: {e}"

    # Stage 2: Use an LLM for remaining uncategorized transactions.
    logger.info("Stage 2: Fetching uncategorized transactions for LLM.")
    select_uncategorized_query = """
    SELECT transaction_id, description_cleaned, merchant_name_cleaned
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE primary_category IS NULL
    LIMIT 100;
    """
    try:
        uncategorized_df = client.query(select_uncategorized_query).to_dataframe()
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error retrieving uncategorized transactions: {e}")
        return f"🚨 Failed to retrieve uncategorized transactions: {e}"

    if uncategorized_df.empty:
        logger.info("✅ No new transactions found requiring LLM categorization.")
        return "✅ Categorization complete. No new transactions required LLM categorization."

    logger.info(f"Found {len(uncategorized_df)} transactions to categorize with the LLM.")
    model = GenerativeModel("gemini-2.5-flash")

    # --- START: Enhanced Prompt ---
    prompt = f"""
    You are an expert financial transaction categorizer. Your task is to categorize the transactions in the following JSON data.

    **Instructions:**
    1.  For each transaction object, determine the correct `primary_category` and `secondary_category`.
    2.  You **MUST** use only the categories provided in the "Valid Categories" section below.
    3.  Your final output **MUST** be a valid JSON array of objects.
    4.  Each object in the array **MUST** contain three keys: `transaction_id`, `primary_category`, and `secondary_category`.

    **Valid Categories:**
    ```json
    {json.dumps(VALID_CATEGORIES, indent=4)}
    ```

    **Example Output Format:**
    ```json
    [
      {{
        "transaction_id": "TXN-EXAMPLE-123",
        "primary_category": "Expense",
        "secondary_category": "Groceries"
      }},
      {{
        "transaction_id": "TXN-EXAMPLE-456",
        "primary_category": "Income",
        "secondary_category": "Payroll"
      }}
    ]
    ```

    **Transactions to Categorize:**
    ```json
    {uncategorized_df.to_json(orient='records')}
    ```
    """
    # --- END: Enhanced Prompt ---


    response_text = ""
    categorized_data = []
    try:
        logger.info("Sending batch to Gemini for categorization...")
        response = model.generate_content(prompt)
        response_text = response.text
        cleaned_response = response_text.strip().replace('```json', '').replace('```', '').strip()
        
        # --- Validation Logic ---
        parsed_json = json.loads(cleaned_response)

        if not isinstance(parsed_json, list):
            logger.warning(f"LLM response was not a list, but a {type(parsed_json)}. Wrapping in a list.")
            parsed_json = [parsed_json]
        
        for item in parsed_json:
            if (isinstance(item, dict) and
                    'transaction_id' in item and
                    'primary_category' in item and
                    'secondary_category' in item and
                    isinstance(item.get('transaction_id'), str) and
                    isinstance(item.get('primary_category'), str) and
                    isinstance(item.get('secondary_category'), str)):
                categorized_data.append(item)
            else:
                logger.warning(f"Skipping invalid record from LLM: {item}")
        # --- End Validation Logic ---

        logger.info(f"Received and validated {len(categorized_data)} categorizations from LLM.")

    except (json.JSONDecodeError, AttributeError, Exception) as e:
        logger.error(f"🚨 Failed to parse or validate LLM response: {e}. Raw response: {response_text}")
        return f"🚨 Failed to parse or validate LLM response: {e}. Response was: {response_text}"

    if not categorized_data:
        logger.warning("LLM categorization ran, but no new valid category suggestions were produced.")
        return "🤔 LLM categorization ran, but no new valid category suggestions were produced."

    # Stage 3: Update BigQuery with a secure, parameterized query
    logger.info("Stage 3: Applying LLM-based categorizations to BigQuery.")
    llm_merge_query = f"""
    MERGE `fsi-banking-agentspace.txns.transactions` AS T
    USING UNNEST(@updates) AS S
    ON T.transaction_id = S.transaction_id
    WHEN MATCHED AND T.primary_category IS NULL THEN
        UPDATE SET
            primary_category = S.primary_category,
            secondary_category = S.secondary_category,
            categorization_method = 'llm-powered'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "updates",
                "STRUCT<transaction_id STRING, primary_category STRING, secondary_category STRING>",
                categorized_data
            )
        ]
    )

    try:
        llm_merge_job = client.query(llm_merge_query, job_config=job_config)
        llm_merge_job.result()
        updated_count = llm_merge_job.num_dml_affected_rows or 0
        logger.info(f"✅ Successfully updated {updated_count} transactions with LLM categories.")
        return f"✅ Categorization complete! Rules were applied, and the LLM categorized an additional {updated_count} transactions."
    except GoogleAPICallError as e:
        logger.error(f"🚨 BigQuery error during LLM-based categorization update: {e}")
        return f"🚨 An error occurred during LLM-based categorization: {e}"