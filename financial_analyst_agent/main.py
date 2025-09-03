import os
import sys
from google.cloud.aiplatform import reasoning_engines
from src.fin_agent.agents.root import root_agent

# For local development, it's common to use a .env file.
# To enable this, you would uncomment the following lines and add `python-dotenv`
# to your requirements.txt file.
# from dotenv import load_dotenv
# load_dotenv()

# --- 1. Agent Packaging ---
# The root_agent, which orchestrates all other agents and tools, is wrapped
# in an AdkApp. This is the object that will be containerized and deployed.
# Tracing is enabled as specified in the TDD for monitoring and debugging.
app = reasoning_engines.AdkApp(
    agent=root_agent,
    enable_tracing=True
)

# --- 2. Deployment Logic ---
def deploy():
    """
    Handles the deployment of the packaged agent to Vertex AI Reasoning Engines.
    Reads configuration from environment variables.
    """
    # Deployment requires the GCP Project ID and a Cloud Storage bucket for staging.
    project_id = os.environ.get("GCP_PROJECT_ID")
    location = os.environ.get("GCP_LOCATION", "us-central1")
    staging_bucket = os.environ.get("GCP_STAGING_BUCKET")
    engine_display_name = "financial-transaction-analyst-agent"

    if not all([project_id, staging_bucket]):
        raise ValueError(
            "The GCP_PROJECT_ID and GCP_STAGING_BUCKET environment variables "
            "are required for deployment."
        )

    print(f"Deploying '{engine_display_name}' to project '{project_id}' in '{location}'...")

    # This function packages the agent, containerizes it, pushes it to
    # Google Artifact Registry, and provisions the serving infrastructure on Vertex AI.
    engine = reasoning_engines.ReasoningEngine.create(
        app,
        display_name=engine_display_name,
        location=location,
        project=project_id,
        staging_bucket=staging_bucket,
    )

    print(f"Deployment successful! Reasoning Engine resource name: {engine.resource_name}")
    return engine

if __name__ == "__main__":
    # This block allows the script to be executed to trigger deployment.
    # The user must be authenticated (`gcloud auth application-default login`)
    # and have the necessary environment variables set.
    # To trigger deployment, run: `python main.py deploy`
    if len(sys.argv) > 1 and sys.argv[1] == 'deploy':
        deploy()
    else:
        print("Main entry point executed. To deploy the agent, run 'python main.py deploy'.")
        print("Ensure GCP_PROJECT_ID and GCP_STAGING_BUCKET environment variables are set.")
