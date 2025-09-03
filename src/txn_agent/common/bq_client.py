from google.adk.tools.bigquery import BigQueryToolset, BigQueryCredentialsConfig
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
import google.auth

def get_bq_toolset(read_only: bool = False) -> BigQueryToolset:
    """Creates a configured BigQueryToolset."""
    write_mode = WriteMode.BLOCKED if read_only else WriteMode.ALLOWED
    tool_config = BigQueryToolConfig(write_mode=write_mode)

    credentials, _ = google.auth.default()
    credentials_config = BigQueryCredentialsConfig(credentials=credentials)

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
        # ... (rest of the schema)
    ]

    rules_schema = [
        bigquery.SchemaField("rule_id", "INT64", mode="REQUIRED"),
        # ... (rest of the schema)
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

def get_bq_toolset(read_only: bool = False) -> BigQueryToolset:
    """Creates a configured BigQueryToolset."""
    # ... (rest of the function)
    setup_bigquery_tables(bq_client) # Call the new function
    return BigQueryToolset(
        # ...
    )