# src/txn_agent/tools/cancellation_tools.py

from src.txn_agent.common.cancellation import cancellation_token

def request_cancellation() -> str:
    """Requests to cancel the ongoing operation."""
    cancellation_token.request_cancellation()
    return "✅ Cancellation requested. The current process will stop shortly."