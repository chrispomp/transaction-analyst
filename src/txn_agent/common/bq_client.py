from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.adk.tools.bigquery import BigQueryToolset, BigQueryCredentialsConfig
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
import google.auth

def get_bq_toolset(read_only: bool = False) -> BigQueryToolset:
    """Creates a configured BigQueryToolset."""
    write_mode = WriteMode.BLOCKED if read_only else WriteMode.ALLOWED
    tool_config = BigQueryToolConfig(write_mode=write_mode)

    credentials, _ = google.auth.default()
    credentials_config = BigQueryCredentialsConfig(credentials=credentials)

    # Create a BigQuery client to pass to the setup function
    bq_client = bigquery.Client(credentials=credentials)
    setup_bigquery_tables(bq_client) # Call the new function

    return BigQueryToolset(
        bigquery_tool_config=tool_config,
        credentials_config=credentials_config
    )

def setup_bigquery_tables(bq_client):
    """Creates the necessary BigQuery tables if they don't exist."""
    dataset_id = "fsi-banking-agentspace.txns"
    transactions_table_id = f"{dataset_id}.transactions"
    rules_table_id = f"{dataset_id}.rules"

    transactions_schema = [
        bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("consumer_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("persona_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("institution_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("account_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("transaction_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("transaction_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("is_recurring", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("description_raw", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description_cleaned", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("merchant_name_raw", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("merchant_name_cleaned", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("primary_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("secondary_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("channel", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("categorization_update_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("categorization_method", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rule_id", "STRING", mode="NULLABLE")
    ]

    rules_schema = [
        bigquery.SchemaField("rule_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("primary_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("secondary_category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("merchant_name_cleaned_match", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("persona_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("confidence_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    ]


    try:
        bq_client.get_table(transactions_table_id)
    except NotFound:
        table = bigquery.Table(transactions_table_id, schema=transactions_schema)
        bq_client.create_table(table)

    try:
        bq_client.get_table(rules_table_id)
    except NotFound:
        table = bigquery.Table(rules_table_id, schema=rules_schema)
        bq_client.create_table(table)