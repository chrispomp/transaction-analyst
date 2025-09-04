# src/txn_agent/tools/analyst_tools.py

from google.cloud import bigquery
from typing import Literal

def execute_sql(query: str) -> str:
    """
    Executes a read-only SQL query against the BigQuery database and returns the results
    in a markdown table.
    """
    client = bigquery.Client()
    try:
        query_job = client.query(query)
        results = query_job.to_dataframe()
        return results.to_markdown()
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def execute_confirmed_update(query: str, confirmation: Literal["CONFIRM"]) -> str:
    """
    Executes a data-modifying SQL query (INSERT, UPDATE, DELETE) after explicit
    user confirmation.
    """
    if confirmation != "CONFIRM":
        return "⚠️ **Confirmation Required**: To execute this query, please provide 'CONFIRM'."
    
    client = bigquery.Client()
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        return f"✅ **Success!** The query was executed and affected {query_job.num_dml_affected_rows} rows."
    except Exception as e:
        return f"🚨 **Update Failed**: {e}"