# src/txn_agent/common/constants.py

VALID_CATEGORIES = {
    "Income": ["Gig Income", "Payroll", "Other Income", "Refund", "Interest Income"],
    "Expense": [
        "Groceries", "Pharmacy", "Office Supplies", "Food & Dining", "Coffee Shop", "Shopping", "Entertainment",
        "Health & Wellness", "Auto & Transport", "Travel & Vacation",
        "Software & Tech", "Medical", "Insurance", "Bills & Utilities",
        "Fees & Charges", "Business Services", "Other Expense", "Loan Payment"
    ],
    "Transfer": ["Credit Card Payment", "Internal Transfer", "ATM Withdrawal"]
}