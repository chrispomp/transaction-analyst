# Generates high-fidelity, narratively cohesive synthetic data for testing
# AI/ML models for credit scoring based on transaction history.
# Version 8.2 - Added 'Peer-to-Peer Debit' expense category.

import os
import json
import uuid
import random
import logging
import asyncio
import time
import re
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional, Tuple

# Faker for realistic data generation
from faker import Faker
# NumPy for statistical modeling
import numpy as np

# Using Vertex AI SDK for GCP integration
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.cloud import bigquery
from google.api_core import exceptions
from google.api_core import retry_async

# --- Configuration ---
PROJECT_ID = os.getenv("PROJECT_ID", "fsi-banking-agentspace")
LOCATION = os.getenv("LOCATION", "us-central1")
DATASET_ID = os.getenv("DATASET_ID", "txns")
TABLE_ID = "transactions"

# --- Generation Parameters ---
NUM_CONSUMERS_TO_GENERATE = 5
MIN_VARIABLE_TRANSACTIONS_PER_MONTH = 25
MAX_VARIABLE_TRANSACTIONS_PER_MONTH = 35
TRANSACTION_HISTORY_MONTHS = 24
CONCURRENT_CONSUMER_JOBS = 10


# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Initialize Faker ---
fake = Faker()

# --- I. CANONICAL DATA STRUCTURES ---

VALID_CATEGORIES = {
    "Income": ["Gig Income", "Payroll", "Other Income", "Refund", "Interest Income", "Peer-to-Peer Credit"],
    "Expense": [
        "Groceries", "Pharmacy", "Office Supplies", "Food & Dining", "Coffee Shop", "Shopping", "Entertainment",
        "Health & Wellness", "Auto & Transport", "Travel & Vacation", "Loan Payment", "Rent Payment",
        "Software & Tech", "Medical", "Insurance", "Bills & Utilities", "ATM Withdrawal",
        "Fees & Charges", "Business Services", "Other Expense", "Mortgage Payment", "Streaming Services",
        "Peer-to-Peer Debit" # MODIFICATION: Added new category
    ]
}

INSTITUTION_NAMES = ["Capital One", "Chase", "Ally Bank", "Bank of America"]
ACCOUNT_TYPES = ["Credit Card", "Checking Account", "Savings Account"]
CHANNELS = ["ATM", "Point-of-Sale", "Card-Not-Present", "Wire Transfer", "ACH", "Check", "P2P", "Internal Transfer"]

EXPENSE_TAXONOMY = [
    {"secondary_category": "Groceries", "merchant": "Whole Foods Market", "channel": "Point-of-Sale"},
    {"secondary_category": "Groceries", "merchant": "Trader Joe's", "channel": "Point-of-Sale"},
    {"secondary_category": "Coffee Shop", "merchant": "Blue Bottle Coffee", "channel": "Point-of-Sale"},
    {"secondary_category": "Food & Dining", "merchant": "Chipotle", "channel": "Point-of-Sale"},
    {"secondary_category": "Shopping", "merchant": "Amazon.com", "channel": "Card-Not-Present"},
    {"secondary_category": "Shopping", "merchant": "Lululemon", "channel": "Card-Not-Present"},
    {"secondary_category": "Streaming Services", "merchant": "Netflix.com", "channel": "Card-Not-Present"},
    {"secondary_category": "Streaming Services", "merchant": "Spotify", "channel": "Card-Not-Present"},
    {"secondary_category": "Pharmacy", "merchant": "CVS Pharmacy", "channel": "Point-of-Sale"},
    {"secondary_category": "Health & Wellness", "merchant": "SilverSneakers Fitness", "channel": "ACH"},
    {"secondary_category": "Auto & Transport", "merchant": "Uber", "channel": "Card-Not-Present"},
    {"secondary_category": "Loan Payment", "merchant": "ALLY FINANCIAL AUTO", "channel": "ACH"},
    {"secondary_category": "Loan Payment", "merchant": "TESLA MOTORS PMT", "channel": "ACH"},
    {"secondary_category": "Travel & Vacation", "merchant": "Expedia", "channel": "Card-Not-Present"},
    {"secondary_category": "Travel & Vacation", "merchant": "Carnival Cruise Line", "channel": "Card-Not-Present"},
    {"secondary_category": "Auto & Transport", "merchant": "Shell Gas Station", "channel": "Point-of-Sale"},
    {"secondary_category": "Software & Tech", "merchant": "ADOBE INC", "channel": "Card-Not-Present"},
    {"secondary_category": "Office Supplies", "merchant": "Staples", "channel": "Point-of-Sale"},
    {"secondary_category": "Shopping", "merchant": "Home Depot", "channel": "Point-of-Sale"},
    {"secondary_category": "Shopping", "merchant": "University Bookstore", "channel": "Point-of-Sale"},
    {"secondary_category": "Medical", "merchant": "City Hospital", "channel": "ACH"},
    {"secondary_category": "Insurance", "merchant": "GEICO", "channel": "ACH"},
    {"secondary_category": "Insurance", "merchant": "STATE FARM INS", "channel": "ACH"},
    {"secondary_category": "Insurance", "merchant": "ALLSTATE HOME & AUTO", "channel": "ACH"},
    {"secondary_category": "Bills & Utilities", "merchant": "T-MOBILE", "channel": "ACH"},
    {"secondary_category": "Bills & Utilities", "merchant": "VERIZON FIOS", "channel": "ACH"},
    {"secondary_category": "Bills & Utilities", "merchant": "AT&T MOBILITY", "channel": "ACH"},
    {"secondary_category": "Bills & Utilities", "merchant": "CON EDISON UTILITY", "channel": "ACH"},
    {"secondary_category": "Bills & Utilities", "merchant": "PG&E UTILITIES", "channel": "ACH"},
    {"secondary_category": "Bills & Utilities", "merchant": "FLORIDA POWER & LIGHT", "channel": "ACH"},
    {"secondary_category": "Fees & Charges", "merchant": "AARP", "channel": "ACH"},
]

MERCHANT_TO_CHANNEL_MAP = {item['merchant']: item['channel'] for item in EXPENSE_TAXONOMY}

LIFE_EVENT_IMPACT_MATRIX = {
    "Unexpected Major Car Repair": {
        "category": "Negative Financial Shock", "magnitude_range": (-2500, -1000), "duration": 2,
        "primary_signature": {"secondary_category": "Auto & Transport", "merchant_options": ["Firestone Auto Care", "Pep Boys", "Local Mechanic LLC"]},
        "secondary_effects_prompt": "Reflect a period of reduced discretionary spending (less Food & Dining, Shopping, Entertainment) for the next 2 months to recover from the car repair cost."
    },
    "Significant Medical Bill": {
        "category": "Negative Financial Shock", "magnitude_range": (-3000, -500), "duration": 3,
        "primary_signature": {"secondary_category": "Medical", "merchant_options": ["Local Hospital Billing", "Specialist Co-Pay", "Out-of-Network Dr."]},
        "secondary_effects_prompt": "Significantly reduce non-essential spending for the next 3 months to accommodate this large, unexpected medical cost."
    },
    "Annual Work Bonus": {
        "category": "Positive Financial Shock", "magnitude_range": (2000, 5000), "duration": 2,
        "primary_signature": {"secondary_category": "Payroll", "merchant_options": ["ADP PAYROLL BONUS", "COMPANY BONUS DIRECT DEPOSIT"]},
        "secondary_effects_prompt": "Reflect a temporary increase in high-ticket or premium spending (e.g., Travel & Vacation, Shopping) for the next 2 months following the bonus."
    },
}

PERSONAS = [
    {
        "persona_name": "Full-Time Rideshare Driver",
        "income_type": "Gig Income", "income_merchants": ["UBER", "LYFT", "DOORDASH"],
        "recurring_expenses": [
            {"merchant_name": "CITY PROPERTY MGMT", "day_of_month": 1, "amount_mean": -1450.00, "amount_std": 25.0, "secondary_category": "Rent Payment"},
            {"merchant_name": "GEICO", "day_of_month": 1, "amount_mean": -180.50, "amount_std": 10.0, "secondary_category": "Insurance"},
            {"merchant_name": "ALLY FINANCIAL AUTO", "day_of_month": 10, "amount_mean": -450.00, "amount_std": 0, "secondary_category": "Loan Payment"},
            {"merchant_name": "T-MOBILE", "day_of_month": 15, "amount_mean": -95.00, "amount_std": 5.0, "secondary_category": "Bills & Utilities"},
        ]
    },
    {
        "persona_name": "Freelance Creative",
        "income_type": "Gig Income", "income_merchants": ["STRIPE", "UPWORK"],
        "recurring_expenses": [
            {"merchant_name": "EQUITY RESIDENTIAL RENT", "day_of_month": 1, "amount_mean": -1900.00, "amount_std": 0, "secondary_category": "Rent Payment"},
            {"merchant_name": "ADOBE INC", "day_of_month": 5, "amount_mean": -59.99, "amount_std": 0, "secondary_category": "Software & Tech"},
            {"merchant_name": "VERIZON FIOS", "day_of_month": 18, "amount_mean": -89.99, "amount_std": 0, "secondary_category": "Bills & Utilities"},
            {"merchant_name": "CON EDISON UTILITY", "day_of_month": 20, "amount_mean": -120.00, "amount_std": 30.0, "secondary_category": "Bills & Utilities"},
        ]
    },
    {
        "persona_name": "Salaried Tech Professional",
        "income_type": "Payroll", "income_merchants": ["ADP", "GOOGLE", "AMAZON WEB SERVICES"],
        "recurring_expenses": [
            {"merchant_name": "WELLS FARGO HOME MTG", "day_of_month": 1, "amount_mean": -3200.00, "amount_std": 0, "secondary_category": "Mortgage Payment"},
            {"merchant_name": "STATE FARM INS", "day_of_month": 1, "amount_mean": -210.00, "amount_std": 15.0, "secondary_category": "Insurance"},
            {"merchant_name": "TESLA MOTORS PMT", "day_of_month": 5, "amount_mean": -750.00, "amount_std": 0, "secondary_category": "Loan Payment"},
            {"merchant_name": "Netflix.com", "day_of_month": 10, "amount_mean": -15.49, "amount_std": 0, "secondary_category": "Streaming Services"},
            {"merchant_name": "PG&E UTILITIES", "day_of_month": 22, "amount_mean": -250.00, "amount_std": 50.0, "secondary_category": "Bills & Utilities"},
        ]
    },
    {
        "persona_name": "University Student",
        "income_type": "Payroll", "income_merchants": ["UNIVERSITY PAYROLL", "NELNET", "SALLIE MAE"],
        "recurring_expenses": [
            {"merchant_name": "STATE UNIVERSITY HOUSING", "day_of_month": 1, "amount_mean": -850.00, "amount_std": 0, "secondary_category": "Rent Payment"},
            {"merchant_name": "AT&T MOBILITY", "day_of_month": 15, "amount_mean": -75.00, "amount_std": 5.0, "secondary_category": "Bills & Utilities"},
            {"merchant_name": "Spotify", "day_of_month": 20, "amount_mean": -10.99, "amount_std": 0, "secondary_category": "Streaming Services"},
        ]
    },
    {
        "persona_name": "Retiree on Fixed Income",
        "income_type": "Other Income", "income_merchants": ["US TREASURY 310", "STATE PENSION FUND"],
        "recurring_expenses": [
            {"merchant_name": "BANK OF AMERICA MORTGAGE", "day_of_month": 1, "amount_mean": -1850.00, "amount_std": 0, "secondary_category": "Mortgage Payment"},
            {"merchant_name": "ALLSTATE HOME & AUTO", "day_of_month": 1, "amount_mean": -240.00, "amount_std": 0, "secondary_category": "Insurance"},
            {"merchant_name": "SilverSneakers Fitness", "day_of_month": 5, "amount_mean": -25.00, "amount_std": 0, "secondary_category": "Health & Wellness"},
            {"merchant_name": "AARP", "day_of_month": 12, "amount_mean": -16.00, "amount_std": 0, "secondary_category": "Fees & Charges"},
            {"merchant_name": "FLORIDA POWER & LIGHT", "day_of_month": 18, "amount_mean": -180.00, "amount_std": 45.0, "secondary_category": "Bills & Utilities"},
        ]
    },
]

# --- II. HYBRID GENERATION & STATISTICAL MODELING ---

AMOUNT_DISTRIBUTIONS = {
    # Expenses
    "Groceries": {"log_mean": 3.8, "log_std": 0.6}, "Food & Dining": {"log_mean": 2.8, "log_std": 0.8},
    "Coffee Shop": {"log_mean": 2.0, "log_std": 0.5}, "Shopping": {"log_mean": 4.2, "log_std": 1.0},
    "Streaming Services": {"log_mean": 2.8, "log_std": 0.3}, "Entertainment": {"log_mean": 3.5, "log_std": 0.8},
    "Health & Wellness": {"log_mean": 3.5, "log_std": 0.8}, "Pharmacy": {"log_mean": 3.2, "log_std": 0.7},
    "Auto & Transport": {"log_mean": 3.4, "log_std": 0.9}, "Travel & Vacation": {"log_mean": 5.5, "log_std": 0.8},
    "Software & Tech": {"log_mean": 4.0, "log_std": 1.0}, "Medical": {"log_mean": 4.5, "log_std": 1.1},
    "Office Supplies": {"log_mean": 3.7, "log_std": 0.9}, "Fees & Charges": {"log_mean": 2.9, "log_std": 0.5},
    "Loan Payment": {"log_mean": 6.0, "log_std": 0.2}, "Mortgage Payment": {"log_mean": 7.5, "log_std": 0.2},
    "Rent Payment": {"log_mean": 7.2, "log_std": 0.3}, "Insurance": {"log_mean": 5.0, "log_std": 0.4},
    "Bills & Utilities": {"log_mean": 4.5, "log_std": 0.5},
    "Peer-to-Peer Debit": {"log_mean": 4.8, "log_std": 1.0}, # MODIFICATION: Added distribution
    # Income
    "Gig Income": {"log_mean": 6.0, "log_std": 0.8}, "Payroll": {"log_mean": 7.8, "log_std": 0.2},
    "Interest Income": {"log_mean": 2.5, "log_std": 0.4}, "Refund": {"log_mean": 4.0, "log_std": 0.9},
    "Other Income": {"log_mean": 5.0, "log_std": 1.2}, "Peer-to-Peer Credit": {"log_mean": 4.8, "log_std": 1.0},
    # Default
    "Default": {"log_mean": 3.0, "log_std": 1.0}
}

WEEKDAY_HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 5, 8, 9, 7, 5, 6, 10, 10, 8, 6, 7, 9, 9, 8, 6, 4, 3, 2]
WEEKEND_HOUR_WEIGHTS = [2, 2, 1, 1, 1, 2, 3, 4, 6, 8, 10, 10, 9, 8, 7, 7, 8, 9, 9, 8, 6, 5, 4, 3]

def generate_realistic_amount(secondary_category: str) -> float:
    params = AMOUNT_DISTRIBUTIONS.get(secondary_category, AMOUNT_DISTRIBUTIONS["Default"])
    amount = np.random.lognormal(mean=params['log_mean'], sigma=params['log_std'])
    return round(amount, 2)

def generate_realistic_timestamp(base_date: datetime) -> datetime:
    is_weekday = base_date.weekday() < 5
    weights = WEEKDAY_HOUR_WEIGHTS if is_weekday else WEEKEND_HOUR_WEIGHTS
    hour = random.choices(range(24), weights=weights, k=1)[0]
    minute, second = random.randint(0, 59), random.randint(0, 59)
    return base_date.replace(hour=hour, minute=minute, second=second, microsecond=0)

def clean_description(raw_desc: str) -> str:
    if not isinstance(raw_desc, str):
        return ""
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', raw_desc).lower()
    return re.sub(r'\s+', ' ', cleaned).strip()

# --- III. PROMPT ENGINEERING & SCHEMA DEFINITION ---

TRANSACTION_SCHEMA_FOR_LLM = {
    "type": "array", "items": {
        "type": "object", "properties": {
            "description_raw": {"type": "string", "description": "The complete, raw transaction line item. MUST include extra details beyond just the merchant name, such as transaction prefixes (SQ*, TST*), store numbers, location, or reference codes."},
            "merchant_name_raw": {"type": "string", "description": "The raw merchant name as extracted from the description (e.g., 'SQ *BLUE BOTTLE COFFEE #B12')."},
            "merchant_name_cleaned": {"type": "string", "description": "The cleaned, canonical merchant name (e.g., 'Blue Bottle Coffee')."},
            "secondary_category": {"type": "string", "enum": VALID_CATEGORIES["Income"] + VALID_CATEGORIES["Expense"], "description": "The detailed sub-category of the transaction."}
        }, "required": ["description_raw", "merchant_name_raw", "merchant_name_cleaned", "secondary_category"]
    }
}

def build_monthly_prompt(profile: Dict, month_date: datetime, transactions_this_month: int, active_event: Optional[Dict], seasonal_context: Optional[str]) -> str:
    month_name = month_date.strftime("%B %Y")
    persona = profile['persona']
    narrative_block = "This was a typical month."
    if active_event:
        narrative_block = f"CRITICAL NARRATIVE EVENT: This month, the consumer is dealing with '{active_event['name']}'. {active_event['details']['secondary_effects_prompt']}"
    elif seasonal_context:
        narrative_block = f"SEASONAL CONTEXT: {seasonal_context}"

    income_merchant_example = random.choice(persona.get("income_merchants", ["DEPOSIT"]))
    
    # Dynamically list some relevant expense categories for the persona
    persona_expense_categories = random.sample(VALID_CATEGORIES["Expense"], k=5)
    
    few_shot_examples = f"""
    **High-Quality Output Examples (for style guidance only):**
    ```json
    [
        {{"description_raw": "SQ *BLUE BOTTLE COFFEE #B12", "merchant_name_raw": "SQ *BLUE BOTTLE COFFEE", "merchant_name_cleaned": "Blue Bottle Coffee", "secondary_category": "Coffee Shop"}},
        {{"description_raw": "POS Debit TRADER JOE'S #552 PHOENIX AZ", "merchant_name_raw": "TRADER JOE'S #552", "merchant_name_cleaned": "Trader Joe's", "secondary_category": "Groceries"}},
        {{"description_raw": "AMAZON.COM*A12B34CD5 AMZN.COM/BILL WA", "merchant_name_raw": "AMAZON.COM*A12B34CD5", "merchant_name_cleaned": "Amazon.com", "secondary_category": "Shopping"}},
        {{"description_raw": "UBER TRIP 6J7K8L HELP.UBER.COM", "merchant_name_raw": "UBER TRIP", "merchant_name_cleaned": "Uber", "secondary_category": "Auto & Transport"}},
        {{"description_raw": "{income_merchant_example} DEPOSIT PMT_1234", "merchant_name_raw": "{income_merchant_example} DEPOSIT", "merchant_name_cleaned": "{income_merchant_example}", "secondary_category": "{persona['income_type']}"}}
    ]
    ```
    """
    
    return f"""
    Generate a flat JSON array of exactly {transactions_this_month} realistic, variable bank transactions for '{profile["consumer_name"]}' for **{month_name}**.
    - Persona: '{persona["persona_name"]}'.
    - **Income Focus:** Primary income should be categorized as '{persona['income_type']}' from sources like: {', '.join(persona['income_merchants'])}. Also include other income types like 'Refund' or 'Peer-to-Peer Credit' where appropriate.
    - **Expense Focus:** Generate transactions reflecting spending in categories like: {', '.join(persona_expense_categories)}. Also include 'Peer-to-Peer Debit' transactions where logical (e.g., paying a friend).
    - **Monthly Narrative:** {narrative_block}
    **CRITICAL INSTRUCTIONS:**
    1.  **Use Valid Categories:** The `secondary_category` MUST be one of the allowed values.
    2.  **Differentiate Descriptions:** The `description_raw` MUST be more detailed than `merchant_name_raw`. It should contain the merchant name plus other realistic data like store numbers, transaction type prefixes (e.g. 'SQ *', 'POS Debit'), locations, or reference IDs. For Peer-to-Peer transactions, the merchant name should be a person's name.
    3.  The `merchant_name_raw` MUST be a logical, consistent part of the `description_raw`.
    4.  The `merchant_name_cleaned` MUST be the canonical, recognizable name of the business or person.
    5.  Do NOT generate predictable monthly bills; they are handled separately.
    6.  The entire output MUST be ONLY the raw JSON array, conforming strictly to the provided schema.
    {few_shot_examples}
    **Your Task:** Generate the JSON array for **{month_name}**.
    """

# --- IV. CLIENT & BQ SETUP ---

try:
    if not PROJECT_ID or "your-gcp-project-id" in PROJECT_ID:
        raise ValueError("PROJECT_ID is not set correctly.")
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    model = GenerativeModel("gemini-2.5-flash")
    bq_client = bigquery.Client(project=PROJECT_ID)
    logging.info(f"Initialized Vertex AI and BigQuery for project '{PROJECT_ID}'")
except Exception as e:
    logging.error(f"Failed to initialize Google Cloud clients: {e}. Ensure you are authenticated ('gcloud auth application-default login').")
    exit()

TRANSACTIONS_SCHEMA = [
    bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"), bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("consumer_name", "STRING"), bigquery.SchemaField("persona_type", "STRING"),
    bigquery.SchemaField("institution_name", "STRING"), bigquery.SchemaField("account_type", "STRING"),
    bigquery.SchemaField("transaction_date", "TIMESTAMP"), bigquery.SchemaField("transaction_type", "STRING"),
    bigquery.SchemaField("amount", "FLOAT"), bigquery.SchemaField("is_recurring", "BOOLEAN"),
    bigquery.SchemaField("description_raw", "STRING"), bigquery.SchemaField("description_cleaned", "STRING"),
    bigquery.SchemaField("merchant_name_raw", "STRING"), bigquery.SchemaField("merchant_name_cleaned", "STRING"),
    bigquery.SchemaField("primary_category", "STRING"), bigquery.SchemaField("secondary_category", "STRING"),
    bigquery.SchemaField("channel", "STRING"), bigquery.SchemaField("categorization_update_timestamp", "TIMESTAMP"),
    bigquery.SchemaField("categorization_method", "STRING"), bigquery.SchemaField("rule_id", "STRING")
]

@retry_async.AsyncRetry(predicate=retry_async.if_exception_type(exceptions.Aborted, exceptions.DeadlineExceeded, exceptions.ServiceUnavailable, exceptions.TooManyRequests), initial=1.0, maximum=16.0, multiplier=2.0)
async def generate_data_with_gemini(prompt: str) -> List[Dict[str, Any]]:
    logging.info(f"Sending prompt to Gemini (first 120 chars): {prompt[:120].strip().replace('/n', '')}...")
    try:
        generation_config = GenerationConfig(response_mime_type="application/json", response_schema=TRANSACTION_SCHEMA_FOR_LLM, temperature=1.0, max_output_tokens=8192)
        response = await model.generate_content_async(prompt, generation_config=generation_config)
        text_response = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(text_response)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from Vertex AI API: {e}. Raw response: {response.text}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred with the Vertex AI API: {e}")
        return []

def setup_bigquery_table():
    full_dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = LOCATION
    bq_client.create_dataset(dataset, exists_ok=True)
    logging.info(f"Dataset '{full_dataset_id}' is ready.")
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    bq_client.delete_table(table_ref, not_found_ok=True)
    logging.info(f"Ensured old table '{TABLE_ID}' is removed.")
    table = bigquery.Table(table_ref, schema=TRANSACTIONS_SCHEMA)
    bq_client.create_table(table)
    logging.info(f"Sent request to create table '{TABLE_ID}'.")
    time.sleep(5) # Give BQ time to create the table
    try:
        bq_client.get_table(table_ref)
        logging.info(f"✅ Table '{TABLE_ID}' successfully verified and is ready.")
    except exceptions.NotFound:
        logging.error("Fatal: Failed to verify table creation. Aborting.")
        exit()

def upload_to_bigquery(data: List[Dict[str, Any]]):
    if not data: return
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(schema=TRANSACTIONS_SCHEMA, write_disposition="WRITE_APPEND")
    try:
        logging.info(f"Starting BigQuery Load Job to insert {len(data)} rows into {table_id}...")
        load_job = bq_client.load_table_from_json(data, table_id, job_config=job_config)
        load_job.result()
        logging.info(f"✅ Success! Load Job complete. Loaded {len(data)} rows.")
    except Exception as e:
        logging.error(f"BigQuery Load Job failed: {e}")
        if 'load_job' in locals() and load_job.errors:
            for error in load_job.errors: logging.error(f"  - BQ Error: {error['message']}")
        raise

# --- V. PROGRAMMATIC & HYBRID TRANSACTION LOGIC ---

def generate_life_events(history_months: int) -> List[Dict[str, Any]]:
    events = []
    if history_months <= 1: return events
    num_events = random.randint(1, min(2, history_months - 1))
    used_months = set()
    for _ in range(num_events):
        event_name = random.choice(list(LIFE_EVENT_IMPACT_MATRIX.keys()))
        details = LIFE_EVENT_IMPACT_MATRIX[event_name]
        potential_start = random.randint(1, history_months - 1)
        if not any(m in used_months for m in range(potential_start, potential_start + details['duration'])):
            for m in range(potential_start, potential_start + details['duration']): used_months.add(m)
            events.append({"name": event_name, "details": details, "magnitude": round(random.uniform(*details['magnitude_range']), 2), "start_month_ago": potential_start, "end_month_ago": potential_start - details['duration']})
    return events


def inject_recurring_transactions(profile: Dict, history_months: int) -> List[Dict[str, Any]]:
    persona = profile['persona']
    accounts = profile['accounts']
    checking_account = accounts['Checking Account']
    txns = []
    for bill in persona.get('recurring_expenses', []):
        for i in range(history_months):
            base_date = datetime.now(timezone.utc) - relativedelta(months=i)
            last_day_of_month = (base_date.replace(day=28) + timedelta(days=4)).day
            day_to_use = min(bill['day_of_month'], last_day_of_month)
            date = base_date.replace(day=day_to_use, hour=random.randint(9, 17), minute=random.randint(0, 59))
            amount = round(random.gauss(bill['amount_mean'], bill['amount_std']), 2)
            raw_desc = f"ACH Debit - {bill['merchant_name']}"
            txns.append({
                "transaction_id": f"TXN-{uuid.uuid4()}", "account_id": checking_account['account_id'],
                "consumer_name": profile['consumer_name'], "persona_type": persona['persona_name'],
                "institution_name": checking_account['institution_name'], "account_type": "Checking Account",
                "transaction_date": date.isoformat(), "transaction_type": "Debit", "amount": amount, "is_recurring": True,
                "description_raw": raw_desc, "description_cleaned": clean_description(raw_desc),
                "merchant_name_raw": bill['merchant_name'], "merchant_name_cleaned": bill['merchant_name'],
                "primary_category": "Expense", "secondary_category": bill['secondary_category'], "channel": "ACH",
                "categorization_update_timestamp": datetime.now(timezone.utc).isoformat(),
            })
    return txns

def inject_programmatic_event_transactions(profile: Dict, life_events: List[Dict]) -> List[Dict[str, Any]]:
    txns = []
    accounts = profile['accounts']
    for event in life_events:
        if not (sig := event['details'].get('primary_signature')): continue
        amount = event['magnitude']
        is_credit = amount > 0
        account_type = "Checking Account" if is_credit else random.choice(["Checking Account", "Credit Card"])
        account = accounts.get(account_type, accounts['Checking Account'])
        channel = "ACH" if is_credit else "Card-Not-Present"
        date = (datetime.now(timezone.utc) - relativedelta(months=event['start_month_ago'])).replace(day=random.randint(5, 25), hour=random.randint(10, 16))
        merchant = random.choice(sig['merchant_options'])
        raw_desc = f"EVENT: {event['name'].upper()}"
        txns.append({
            "transaction_id": f"TXN-{uuid.uuid4()}", "account_id": account['account_id'],
            "consumer_name": profile['consumer_name'], "persona_type": profile['persona']['persona_name'],
            "institution_name": account['institution_name'], "account_type": account_type, "transaction_date": date.isoformat(),
            "transaction_type": "Credit" if is_credit else "Debit", "amount": amount, "is_recurring": False,
            "description_raw": raw_desc, "description_cleaned": clean_description(raw_desc),
            "merchant_name_raw": merchant, "merchant_name_cleaned": merchant,
            "primary_category": "Income" if is_credit else "Expense", "secondary_category": sig['secondary_category'], "channel": channel,
            "categorization_update_timestamp": datetime.now(timezone.utc).isoformat(),
        })
    return txns

async def generate_cohesive_txns_for_consumer(profile: Dict, history_months: int, min_txns: int, max_txns: int) -> List[Dict[str, Any]]:
    logging.info(f"Generating full history for '{profile['consumer_name']}'...")
    life_events = generate_life_events(history_months)
    final_txns = inject_recurring_transactions(profile, history_months)
    final_txns.extend(inject_programmatic_event_transactions(profile, life_events))
    
    for i in range(history_months):
        month_date = datetime.now(timezone.utc) - relativedelta(months=i)
        active_event = next((e for e in life_events if e['end_month_ago'] < i <= e['start_month_ago']), None)
        seasonal = "Holiday season spending." if month_date.month in [11, 12] else "Summer vacation spending." if month_date.month in [6, 7, 8] else None
        
        prompt = build_monthly_prompt(profile, month_date, random.randint(min_txns, max_txns), active_event, seasonal)
        monthly_txns_from_llm = await generate_data_with_gemini(prompt)
        
        for txn in monthly_txns_from_llm:
            try:
                cat_l2 = txn['secondary_category']
                is_credit = cat_l2 in VALID_CATEGORIES["Income"]
                
                amount = generate_realistic_amount(cat_l2)
                
                if is_credit:
                    account_type = "Checking Account"
                    transaction_type = "Credit"
                    final_amount = abs(amount)
                    channel = "P2P" if cat_l2 == "Peer-to-Peer Credit" else "ACH"
                else: # Is Expense
                    account_type = random.choice([acc for acc in profile['accounts'].keys() if acc != "Savings Account"])
                    transaction_type = "Debit"
                    final_amount = -abs(amount)
                    # MODIFICATION: Assign correct channel for P2P Debit
                    if cat_l2 == "Peer-to-Peer Debit":
                        channel = "P2P"
                    else:
                        channel = MERCHANT_TO_CHANNEL_MAP.get(txn['merchant_name_cleaned'], "Card-Not-Present")
                
                account = profile['accounts'][account_type]
                
                txn.update({
                    "transaction_id": f"TXN-{uuid.uuid4()}", "account_id": account['account_id'],
                    "consumer_name": profile['consumer_name'], "persona_type": profile['persona']['persona_name'],
                    "institution_name": account['institution_name'], "account_type": account_type,
                    "transaction_date": generate_realistic_timestamp(month_date.replace(day=random.randint(1, 28))).isoformat(),
                    "transaction_type": transaction_type, "amount": final_amount, "is_recurring": False,
                    "description_cleaned": clean_description(txn.get('description_raw')),
                    "primary_category": "Income" if is_credit else "Expense",
                    "channel": channel,
                    "categorization_update_timestamp": datetime.now(timezone.utc).isoformat(),
                })
                final_txns.append(txn)
            except (ValueError, TypeError, KeyError) as e:
                logging.warning(f"Could not process transaction: {e}. Raw txn: {txn}")
    return final_txns


# --- VI. MAIN ORCHESTRATION ---
async def generate_transactions_main(num_consumers: int, min_txns_monthly: int, max_txns_monthly: int, history_months: int, concurrent_jobs: int):
    logging.info(f"--- Starting High-Fidelity Synthetic Data Generation for {num_consumers} Consumers ---")
    setup_bigquery_table()
    
    shuffled_personas = random.sample(PERSONAS, len(PERSONAS))
    consumer_profiles = []
    for i in range(num_consumers):
        institutions = random.sample(INSTITUTION_NAMES, k=min(len(INSTITUTION_NAMES), 2))
        profile = {
            "consumer_name": fake.name(),
            "persona": shuffled_personas[i % len(shuffled_personas)],
            "accounts": {
                "Checking Account": {"account_id": f"ACC-{str(uuid.uuid4())[:12].upper()}", "institution_name": institutions[0]},
                "Credit Card": {"account_id": f"ACC-{str(uuid.uuid4())[:12].upper()}", "institution_name": institutions[1]},
            }
        }
        consumer_profiles.append(profile)

    semaphore = asyncio.Semaphore(concurrent_jobs)
    async def process_and_upload(profile):
        async with semaphore:
            transactions = await generate_cohesive_txns_for_consumer(profile, history_months, min_txns_monthly, max_txns_monthly)
            if transactions:
                upload_to_bigquery(transactions)
                return len(transactions)
            return 0

    tasks = [process_and_upload(profile) for profile in consumer_profiles]
    results = await asyncio.gather(*tasks)
    
    total_generated = sum(results)
    if not total_generated:
        logging.error("No transaction data was generated. Aborting.")
        return

    logging.info(f"Generated a grand total of {total_generated} transactions.")
    logging.info("--- ✅ High-Fidelity Data Generation and Upload Complete! ---")


if __name__ == "__main__":
    try:
        asyncio.run(generate_transactions_main(
            num_consumers=NUM_CONSUMERS_TO_GENERATE,
            min_txns_monthly=MIN_VARIABLE_TRANSACTIONS_PER_MONTH,
            max_txns_monthly=MAX_VARIABLE_TRANSACTIONS_PER_MONTH,
            history_months=TRANSACTION_HISTORY_MONTHS,
            concurrent_jobs=CONCURRENT_CONSUMER_JOBS
        ))
    except Exception as e:
        logging.error(f"The script failed to complete due to an error: {e}")