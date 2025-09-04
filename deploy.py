import os
import sys
import vertexai
from src.txn_agent.agents.root import root_agent
from vertexai import agent_engines
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- 1. Initialization ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
STAGING_BUCKET = os.environ.get("GCP_STAGING_BUCKET")

vertexai.init(
    project=PROJECT_ID,
    location=LOCATION,
    staging_bucket=STAGING_BUCKET,
)

# --- 2. Agent Packaging ---
app = agent_engines.AdkApp(
    agent=root_agent,
    enable_tracing=True,
)