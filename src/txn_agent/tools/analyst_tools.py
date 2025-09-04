# src/txn_agent/tools/analyst_tools.py

from datetime import datetime, timedelta
from typing import Tuple

def calculate_date_range(date_range_str: str) -> str:
    """
    Calculates the start and end dates based on a relative timeframe string
    (e.g., 'Last 3 months'). The agent should use this to set the
    start_date and end_date in its session state.

    Args:
        date_range_str: A string like 'Last 3 months', 'Last 6 months', or 'Last 12 months'.

    Returns:
        A string containing the start and end dates in 'YYYY-MM-DD' format,
        formatted as 'start_date:YYYY-MM-DD, end_date:YYYY-MM-DD'.
    """
    end_date = datetime.now()
    if "3 months" in date_range_str:
        start_date = end_date - timedelta(days=90)
    elif "6 months" in date_range_str:
        start_date = end_date - timedelta(days=180)
    elif "12 months" in date_range_str:
        start_date = end_date - timedelta(days=365)
    else:
        # Default to the last 90 days if the format is unrecognized
        start_date = end_date - timedelta(days=90)
    
    return f"start_date:{start_date.strftime('%Y-%m-%d')}, end_date:{end_date.strftime('%Y-%m-%d')}"