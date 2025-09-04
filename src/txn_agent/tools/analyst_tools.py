# src/txn_agent/tools/analyst_tools.py

from google.cloud import bigquery
import pandas as pd
from typing import Literal

def get_available_personas() -> str:
    """
    Retrieves a distinct list of all persona_type values from the transactions table
    to be presented to the user.
    """
    client = bigquery.Client()
    query = """
    SELECT DISTINCT persona_type
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE persona_type IS NOT NULL
    ORDER BY persona_type;
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return "No personas found in the data."
        
        persona_list = "\n".join([f"{i+1}. {row['persona_type']}" for i, row in results.iterrows()])
        return f"Please select a persona to analyze:\n{persona_list}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def get_consumers_by_persona(persona_type: str) -> str:
    """
    Retrieves a distinct list of consumers for a given persona_type.
    """
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT consumer_name
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE persona_type = '{persona_type}'
    ORDER BY consumer_name;
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return f"No consumers found for the '{persona_type}' persona."
        
        consumer_list = "\n".join([f"{i+1}. {row['consumer_name']}" for i, row in results.iterrows()])
        return f"Please select a consumer to analyze:\n{consumer_list}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def get_persona_for_consumer(consumer_name: str) -> str:
    """
    Retrieves the persona_type for a given consumer_name.
    """
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT persona_type
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE consumer_name = '{consumer_name}';
    """
    try:
        result = client.query(query).to_dataframe()
        if result.empty:
            return f"Could not find a persona for '{consumer_name}'."
        return result['persona_type'][0]
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"


def get_income_sources(consumer_name: str, start_date: str, end_date: str) -> str:
    """
    Identifies all unique sources of income for a specific consumer within a given date range.
    """
    client = bigquery.Client()
    query = f"""
    SELECT DISTINCT merchant_name_cleaned
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE consumer_name = '{consumer_name}'
      AND primary_category = 'Income'
      AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return "No income sources found for this period."
        return f"**Income Sources:**\n{results.to_markdown()}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

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
        SUM(amount) / TIMESTAMP_DIFF('{end_date}', '{start_date}', MONTH) AS average_monthly_income
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
        return f"**Income Summary:**\n{results.to_markdown()}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def calculate_income_stability(consumer_name: str, start_date: str, end_date: str) -> str:
    """
    Analyzes the volatility of a consumer's income over a specified period.
    Calculates the standard deviation of monthly income and the coefficient of variation.
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
        if results.empty or results['average_monthly_income'].iloc[0] is None:
            return "Not enough data to calculate income stability."

        # Add interpretation
        stability_report = f"**Income Stability Analysis:**\n{results.to_markdown()}\n\n"
        cov = results['coefficient_of_variation'].iloc[0]
        if pd.isna(cov):
            stability_report += "ℹ️ **Single Income Month**: Not enough data for a multi-month stability analysis."
        elif cov < 0.15:
            stability_report += "✅ **Highly Stable**: Income shows very little fluctuation month-to-month."
        elif cov < 0.30:
            stability_report += "⚠️ **Moderately Stable**: Some monthly fluctuations detected."
        else:
            stability_report += "🚩 **Highly Variable**: Income fluctuates significantly."
        return stability_report
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def analyze_business_expenses(consumer_name: str, start_date: str, end_date: str) -> str:
    """
    Identifies and summarizes likely business-related expenses for a gig worker.
    This helps differentiate business costs from personal spending to calculate a more accurate net income.
    """
    client = bigquery.Client()
    query = f"""
    SELECT
        secondary_category,
        SUM(amount) AS total_spent,
        COUNT(*) as number_of_transactions
    FROM `fsi-banking-agentspace.txns.transactions`
    WHERE consumer_name = '{consumer_name}'
      AND secondary_category IN ('Auto & Transport', 'Software & Tech', 'Office Supplies', 'Business Services')
      AND transaction_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    ORDER BY total_spent ASC
    """
    try:
        results = client.query(query).to_dataframe()
        if results.empty:
            return "No likely business expenses found in this period."
        return f"**Business Expense Analysis:**\n{results.to_markdown()}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"

def execute_custom_query(query: str, confirmation: Literal["CONFIRM"] | None = None) -> str:
    """
    Executes a custom, read-only SQL query against the database.
    This tool should only be used for questions that cannot be answered by other tools.
    For any data-modifying queries (INSERT, UPDATE, DELETE), you MUST get user confirmation.
    """
    client = bigquery.Client()
    is_write_query = any(keyword in query.upper() for keyword in ['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE', 'DROP', 'ALTER'])

    if is_write_query and confirmation != "CONFIRM":
        return ("🤔 **Confirmation Needed**: This is a data-modifying query. "
                "To protect the data, please review the query and confirm by calling this tool again "
                "with the `confirmation` parameter set to 'CONFIRM'.")

    try:
        results = client.query(query).to_dataframe()
        return f"**Query Results:**\n{results.to_markdown()}"
    except Exception as e:
        return f"🚨 **Query Failed**: {e}"