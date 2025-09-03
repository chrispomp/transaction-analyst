import json
from google.adk.models import Gemini
from src.txn_agent.common.constants import VALID_CATEGORIES
from google.cloud import bigquery

def run_categorization() -> str:
    """
    Categorizes transactions using a hybrid rules-based and LLM-powered approach.
    """
    client = bigquery.Client()

    # Stage 1: Apply existing rules using a MERGE statement.
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
        client.query(rules_merge_query).result()
    except Exception as e:
        return f"🚨 An error occurred during rule-based categorization: {e}"

    # Stage 2: Use an LLM for remaining uncategorized transactions.
    select_uncategorized_query = """
    SELECT transaction_id, description_cleaned, merchant_name_cleaned
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE primary_category IS NULL
    LIMIT 100;
    """
    try:
        uncategorized_df = client.query(select_uncategorized_query).to_dataframe()
    except Exception as e:
        return f"🚨 Failed to retrieve uncategorized transactions: {e}"

    if uncategorized_df.empty:
        return "✅ Categorization complete. No new transactions required LLM categorization."

    model = Gemini(model="gemini-2.5-flash")

    prompt = f"""
    You are an expert financial transaction categorizer. Based on the provided
    JSON data of transactions, determine the correct `primary_category` and
    `secondary_category` for each.

    Please use only the following valid categories:
    {json.dumps(VALID_CATEGORIES, indent=4)}

    Return the output as a valid JSON array of objects,
    where each object contains the `transaction_id`, `primary_category`, and `secondary_category`.

    Example Input:
    [{{"transaction_id": "txn_123", "description_cleaned": "UBER TRIP", "merchant_name_cleaned": "UBER"}}]

    Example Output:
    [{{"transaction_id": "txn_123", "primary_category": "Expense", "secondary_category": "Auto & Transport"}}]

    Transactions to categorize:
    """
    transactions_to_categorize = uncategorized_df[['transaction_id', 'description_cleaned', 'merchant_name_cleaned']].to_dict('records')
    prompt += json.dumps(transactions_to_categorize)

    try:
        response_text = model.predict(prompt).text
        cleaned_response = response_text.strip().replace('```json', '').replace('```', '').strip()
        categorized_data = json.loads(cleaned_response)
    except (json.JSONDecodeError, AttributeError, Exception) as e:
        return f"🚨 Failed to parse LLM response: {e}. Response was: {response_text}"

    if not categorized_data:
        return "🤔 LLM categorization ran, but no new category suggestions were produced."

    values_to_merge = ", ".join([
        f"('{item['transaction_id']}', '{item['primary_category']}', '{item['secondary_category']}')"
        for item in categorized_data
    ])

    llm_merge_query = f"""
    MERGE `fsi-banking-agentspace.txns.transactions` AS T
    USING (
        SELECT * FROM UNNEST([
            STRUCT<transaction_id STRING, primary_category STRING, secondary_category STRING>
            {values_to_merge}
        ])
    ) AS S
    ON T.transaction_id = S.transaction_id
    WHEN MATCHED AND T.primary_category IS NULL THEN
        UPDATE SET
            primary_category = S.primary_category,
            secondary_category = S.secondary_category,
            categorization_method = 'llm-powered'
    """

    try:
        client.query(llm_merge_query).result()
        return f"✅ Categorization complete! Rules were applied, and the LLM categorized an additional {len(categorized_data)} transactions."
    except Exception as e:
        return f"🚨 An error occurred during LLM-based categorization: {e}"