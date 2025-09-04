# src/txn_agent/tools/analyst_tools.py

from google.cloud import bigquery
import pandas as pd
from typing import Literal

# --- CONTEXT & LISTING TOOLS ---

def get_distinct_values(field: Literal["consumer_name", "persona_type"]) -> str:
    """
    Retrieves a distinct list of values for a specified field ('consumer_name' or 'persona_type')
    from the transactions table to be presented to the user as a numbered list.
    """
    client = bigquery.Client()
    if field not in ["consumer_name", "persona_type"]:
        return "🚨 **Invalid Field**: Can only get distinct values for 'consumer_name' or 'persona_type'."
    
    query = f"""
    SELECT DISTINCT {field}
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE {field} IS NOT NULL
    ORDER BY {field};
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return f"No values found for {field}."
        
        item_list = "\n".join([f"{i+1}. {row[field]}" for i, row in results.iterrows()])
        return f"Please select a {field.replace('_', ' ')} to analyze:\n{item_list}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

# --- CONSUMER LEVEL TOOLS ---

def get_consumer_financial_profile(consumer_name: str, start_date: str, end_date: str) -> str:
    """
    Generates a full financial profile for a specific consumer, including income,
    spending, and stability analysis.
    """
    income_summary = summarize_income_by_source(consumer_name, start_date, end_date)
    stability = calculate_income_stability(consumer_name, start_date, end_date)
    # In a real scenario, you would also call a spending analysis tool here.
    return f"**Financial Profile for {consumer_name}:**\n\n{income_summary}\n\n{stability}"


def summarize_income_by_source(consumer_name: str, start_date: str, end_date: str) -> str:
    """
    Calculates the total income from each source and the monthly average for a specific consumer.
    """
    client = bigquery.Client()
    query = f"""
    SELECT
        merchant_name_cleaned AS income_source,
        SUM(amount) AS total_income,
        AVG(amount) AS average_transaction_amount,
        COUNT(*) AS number_of_transactions,
        SUM(amount) / NULLIF(TIMESTAMP_DIFF('{end_date}', '{start_date}', MONTH), 0) AS average_monthly_income
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE consumer_name = '{consumer_name}'
      AND primary_category = 'Income'
      AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    ORDER BY total_income DESC
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return "No income data available to summarize."
        return f"**Income Summary:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def calculate_income_stability(consumer_name: str, start_date: str, end_date: str) -> str:
    """
    Analyzes the volatility of a consumer's income over a specified period.
    """
    client = bigquery.Client()
    query = f"""
    WITH MonthlyIncome AS (
        SELECT
            TIMESTAMP_TRUNC(transaction_date, MONTH) AS income_month,
            SUM(amount) AS monthly_total
        FROM `fsi-banking-agentspace.txns.transactions`
        WHERE consumer_name = '{consumer_name}'
          AND primary_category = 'Income'
          AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
    )
    SELECT
        AVG(monthly_total) AS average_monthly_income,
        STDDEV(monthly_total) AS income_standard_deviation,
        STDDEV(monthly_total) / NULLIF(AVG(monthly_total), 0) AS coefficient_of_variation
    FROM MonthlyIncome
    """
    try:
        results = client.query(query).to_dataframe()
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


# --- PERSONA LEVEL TOOLS ---

def get_persona_financial_snapshot(persona_type: str, start_date: str, end_date: str) -> str:
    """
    Calculates the average financial metrics for all consumers of a specific persona type.
    """
    client = bigquery.Client()
    query = f"""
    WITH ConsumerMonthlyTotals AS (
        SELECT
            consumer_name,
            TIMESTAMP_TRUNC(transaction_date, MONTH) as transaction_month,
            SUM(CASE WHEN transaction_type = 'Credit' THEN amount ELSE 0 END) as monthly_income,
            SUM(CASE WHEN transaction_type = 'Debit' THEN amount ELSE 0 END) as monthly_spending
        FROM `fsi-banking-agentspace.txns.transactions`
        WHERE persona_type = '{persona_type}'
        AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1, 2
    )
    SELECT
        '{persona_type}' as persona_type,
        AVG(monthly_income) as avg_monthly_income,
        AVG(monthly_spending) as avg_monthly_spending,
        STDDEV(monthly_income) as income_volatility,
        COUNT(DISTINCT consumer_name) as number_of_consumers
    FROM ConsumerMonthlyTotals
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return f"No data found for persona '{persona_type}' in this period."
        return f"**Financial Snapshot for {persona_type}:**\n{results.to_markdown(index=False)}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


# --- ALL DATA LEVEL TOOLS ---

def get_system_health_report() -> str:
    """
    Provides a report on the overall data quality and categorization coverage.
    """
    client = bigquery.Client()
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