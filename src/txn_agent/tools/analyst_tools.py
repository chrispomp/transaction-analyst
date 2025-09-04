# src/txn_agent/tools/analyst_tools.py

from google.cloud import bigquery
import pandas as pd
from typing import Literal, Optional
from datetime import datetime, timedelta

# Instantiate the client once for the entire module to use
client = bigquery.Client()

def _parse_date_range(date_range_str: str) -> tuple[str, str]:
    """Converts a relative date string to start and end dates."""
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
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

# --- STATE MANAGEMENT & CONTEXT ---

def get_analysis_context(
    analysis_level: Optional[str] = None,
    context_value: Optional[str] = None,
    date_range: Optional[str] = None
) -> str:
    """
    Manages the conversation flow by checking the analysis state and prompting for the next required piece of information.
    """
    if not analysis_level:
        return (
            "Please choose an analysis level:\n"
            "1. 👤 **Consumer Level**: Analyze a single individual.\n"
            "2. 👥 **Persona Level**: Analyze an aggregate group (e.g., 'Freelance Creative').\n"
            "3. 🌐 **All Data**: Analyze the entire dataset."
        )

    if not context_value:
        if analysis_level == "Consumer Level":
            return get_distinct_values("consumer_name")
        elif analysis_level == "Persona Level":
            return get_distinct_values("persona_type")
        elif analysis_level == "All Data":
            context_value = "All Data" # Automatically set for this level

    if not date_range:
        return "Please provide the time period you'd like to analyze (e.g., Last 3 months, Last 6 months, Last 12 months)."

    # If all context is available, return the main menu for the selected analysis level
    return get_user_selections(analysis_level)


def get_user_selections(analysis_level: str) -> str:
    """
    Returns the appropriate menu of analysis options based on the analysis level.
    """
    if analysis_level == "Consumer Level":
        return (
            "What would you like to see?\n"
            "1. 📄 **Full Financial Profile**: A comprehensive overview of the consumer's financial health.\n"
            "2. 💰 **Income Analysis**: A detailed breakdown of all income sources.\n"
            "3. 📊 **Income Stability Report**: An analysis of income volatility over time.\n"
            "4. 🛍️ **Spending Habits**: A look at the top spending categories and merchants.\n"
            "5. 🌊 **Cash Flow Analysis**: A month-by-month view of income vs. spending."
        )
    elif analysis_level == "Persona Level":
        return (
            "What would you like to see?\n"
            "1.  snapshot **Persona Financial Snapshot**: A high-level summary of the persona's financial metrics.\n"
            "2. 💸 **Average Income Analysis**: An exploration of the persona's typical income sources.\n"
            "3. 📈 **Persona Income Stability Trends**: A report on the income volatility for the persona.\n"
            "4. 🛒 **Common Spending Patterns**: An analysis of the most frequent spending areas for the persona."
        )
    elif analysis_level == "All Data":
        return (
            "What would you like to see?\n"
            "1. ⚙️ **Overall System Health**: A report on data quality and categorization coverage.\n"
            "2. 🔬 **Persona Comparison Report**: A comparative analysis of different personas."
        )


def get_distinct_values(field: Literal["consumer_name", "persona_type"]) -> str:
    """
    Retrieves a distinct list of values for a specified field.
    """
    query = f"SELECT DISTINCT {field} FROM `fsi-banking-agentspace.txns.transactions` WHERE {field} IS NOT NULL ORDER BY {field};"
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return f"No values found for {field}."
        
        item_list = "\n".join([f"{i+1}. {row[field]}" for i, row in results.iterrows()])
        return f"Please select a {field.replace('_', ' ')} to analyze:\n{item_list}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


# --- CONSUMER LEVEL TOOLS ---

def get_consumer_financial_profile(consumer_name: str, date_range: str) -> str:
    """
    Generates a full financial profile for a specific consumer, including income,
    spending, and stability analysis.
    """    
    income_summary = summarize_income_by_source(consumer_name, date_range)
    spending_summary = get_spending_habits(consumer_name, date_range)
    stability = calculate_income_stability(consumer_name, date_range)
    cash_flow = get_cash_flow_analysis(consumer_name, date_range)

    return f"**Financial Profile for {consumer_name}:**\n\n{income_summary}\n\n{spending_summary}\n\n{stability}\n\n{cash_flow}"


def summarize_income_by_source(consumer_name: str, date_range: str) -> str:
    """
    Calculates the total income from each source and the monthly average for a specific consumer.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("consumer_name", "STRING", consumer_name),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    SELECT
        merchant_name_cleaned AS income_source,
        SUM(amount) AS total_income,
        AVG(amount) AS average_transaction_amount,
        COUNT(*) AS number_of_transactions,
        SUM(amount) / NULLIF(TIMESTAMP_DIFF(CAST(@end_date AS TIMESTAMP), CAST(@start_date AS TIMESTAMP), MONTH), 0) AS average_monthly_income
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE UPPER(consumer_name) = UPPER(@consumer_name)
      AND primary_category = 'Income'
      AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
    GROUP BY 1
    ORDER BY total_income DESC
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty:
            return "No income data available to summarize."
        return f"**Income Summary:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def calculate_income_stability(consumer_name: str, date_range: str) -> str:
    """
    Analyzes the volatility of a consumer's income over a specified period.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("consumer_name", "STRING", consumer_name),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    WITH MonthlyIncome AS (
        SELECT
            TIMESTAMP_TRUNC(transaction_date, MONTH) AS income_month,
            SUM(amount) AS monthly_total
        FROM `fsi-banking-agentspace.txns.transactions`
        WHERE UPPER(consumer_name) = UPPER(@consumer_name)
          AND primary_category = 'Income'
          AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
        GROUP BY 1
    )
    SELECT
        AVG(monthly_total) AS average_monthly_income,
        STDDEV(monthly_total) AS income_standard_deviation,
        STDDEV(monthly_total) / NULLIF(AVG(monthly_total), 0) AS coefficient_of_variation
    FROM MonthlyIncome
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty or pd.isna(results['average_monthly_income'].iloc[0]):
            return "Not enough data to calculate income stability."

        stability_report = f"**Income Stability Analysis:**\n{results.to_markdown(index=False)}\n\n"
        cov = results['coefficient_of_variation'].iloc[0]
        if pd.isna(cov):
            stability_report += "ℹ️ **Single Income Month**: Not enough data for a multi-month stability analysis."
        elif cov < 0.15:
            stability_report += "✅ **Highly Stable**: Income shows very little fluctuation."
        elif cov < 0.30:
            stability_report += "⚠️ **Moderately Stable**: Some monthly fluctuations detected."
        else:
            stability_report += "🚩 **Highly Variable**: Income fluctuates significantly."
        return stability_report
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


def get_spending_habits(consumer_name: str, date_range: str) -> str:
    """
    Analyzes a consumer's spending habits by category and merchant.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("consumer_name", "STRING", consumer_name),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    SELECT
        secondary_category,
        SUM(ABS(amount)) as total_spent,
        COUNT(*) as transaction_count
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE UPPER(consumer_name) = UPPER(@consumer_name)
      AND transaction_type = 'Debit'
      AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
    GROUP BY 1
    ORDER BY total_spent DESC
    LIMIT 10;
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty:
            return "No spending data available."
        return f"**Top Spending Categories:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


def get_cash_flow_analysis(consumer_name: str, date_range: str) -> str:
    """
    Provides a month-by-month breakdown of income vs. spending.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("consumer_name", "STRING", consumer_name),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    SELECT
        TIMESTAMP_TRUNC(transaction_date, MONTH) as month,
        SUM(CASE WHEN transaction_type = 'Credit' THEN amount ELSE 0 END) as total_income,
        SUM(CASE WHEN transaction_type = 'Debit' THEN ABS(amount) ELSE 0 END) as total_spending
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE UPPER(consumer_name) = UPPER(@consumer_name)
      AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
    GROUP BY 1
    ORDER BY 1;
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty:
            return "No data for cash flow analysis."
        return f"**Cash Flow Analysis:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

# --- PERSONA LEVEL TOOLS ---

def get_persona_financial_snapshot(persona_type: str, date_range: str) -> str:
    """
    Calculates the average financial metrics for all consumers of a specific persona type.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("persona_type", "STRING", persona_type),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    WITH ConsumerMonthlyTotals AS (
        SELECT
            consumer_name,
            TIMESTAMP_TRUNC(transaction_date, MONTH) as transaction_month,
            SUM(CASE WHEN transaction_type = 'Credit' THEN amount ELSE 0 END) as monthly_income,
            SUM(CASE WHEN transaction_type = 'Debit' THEN amount ELSE 0 END) as monthly_spending
        FROM `fsi-banking-agentspace.txns.transactions`
        WHERE UPPER(persona_type) = UPPER(@persona_type)
        AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
        GROUP BY 1, 2
    )
    SELECT
        @persona_type as persona_type,
        AVG(monthly_income) as avg_monthly_income,
        AVG(monthly_spending) as avg_monthly_spending,
        STDDEV(monthly_income) as income_volatility,
        COUNT(DISTINCT consumer_name) as number_of_consumers
    FROM ConsumerMonthlyTotals
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty:
            return f"No data found for persona '{persona_type}' in this period."
        return f"**Financial Snapshot for {persona_type}:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


def get_persona_spending_patterns(persona_type: str, date_range: str) -> str:
    """
    Analyzes the most common spending categories for a given persona.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("persona_type", "STRING", persona_type),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    SELECT
        secondary_category,
        SUM(ABS(amount)) as total_spent,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT consumer_name) as num_consumers
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE UPPER(persona_type) = UPPER(@persona_type)
      AND transaction_type = 'Debit'
      AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
    GROUP BY 1
    ORDER BY total_spent DESC
    LIMIT 10;
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty:
            return "No spending data available for this persona."
        return f"**Common Spending Patterns for {persona_type}:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


# --- ALL DATA LEVEL TOOLS ---

def get_system_health_report() -> str:
    """
    Provides a report on the overall data quality and categorization coverage.
    """
    query = """
    SELECT
        COUNT(*) as total_transactions,
        COUNTIF(primary_category IS NOT NULL) as categorized_transactions,
        COUNTIF(primary_category IS NULL) as uncategorized_transactions,
        SAFE_DIVIDE(COUNTIF(primary_category IS NOT NULL), COUNT(*)) * 100 as categorization_percentage,
        COUNTIF(categorization_method = 'rule-based') as rule_based_count,
        COUNTIF(categorization_method = 'llm-powered') as llm_powered_count
    FROM `fsi-banking-agentspace.txns.transactions`
    """
    try:
        results = client.query(query).to_dataframe()
        return f"**System Health Report:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


def compare_personas(date_range: str) -> str:
    """
    Compares key financial metrics across all available personas.
    """
    start_date, end_date = _parse_date_range(date_range)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]
    )
    query = """
    WITH PersonaMonthlyStats AS (
      SELECT
        persona_type,
        TIMESTAMP_TRUNC(transaction_date, MONTH) as transaction_month,
        consumer_name,
        SUM(CASE WHEN transaction_type = 'Credit' THEN amount ELSE 0 END) as monthly_income,
        SUM(CASE WHEN transaction_type = 'Debit' THEN ABS(amount) ELSE 0 END) as monthly_spending
      FROM `fsi-banking-agentspace.txns.transactions`
      WHERE persona_type IS NOT NULL
      AND transaction_date BETWEEN CAST(@start_date AS TIMESTAMP) AND CAST(@end_date AS TIMESTAMP)
      GROUP BY 1, 2, 3
    )
    SELECT
      persona_type,
      AVG(monthly_income) as avg_monthly_income,
      AVG(monthly_spending) as avg_monthly_spending,
      STDDEV(monthly_income) as income_volatility,
      COUNT(DISTINCT consumer_name) as num_consumers
    FROM PersonaMonthlyStats
    GROUP BY 1
    ORDER BY avg_monthly_income DESC;
    """
    try:
        results = client.query(query, job_config=job_config).to_dataframe()
        if results.empty:
            return "Not enough data to compare personas."
        return f"**Persona Comparison Report:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"