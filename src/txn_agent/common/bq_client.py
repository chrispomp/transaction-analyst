from google.adk.tools.bigquery import BigQueryToolset, BigQueryCredentialsConfig
from google.adk.tools.bigquery.config import BigQueryToolConfig, WriteMode
import google.auth

def get_bq_toolset(read_only: bool = False) -> BigQueryToolset:
    """Creates a configured BigQueryToolset."""
    write_mode = WriteMode.BLOCKED if read_only else WriteMode.UNRESTRICTED
    tool_config = BigQueryToolConfig(write_mode=write_mode)

    credentials, _ = google.auth.default()
    credentials_config = BigQueryCredentialsConfig(credentials=credentials)

    return BigQueryToolset(
        bigquery_tool_config=tool_config,
        credentials_config=credentials_config
    )