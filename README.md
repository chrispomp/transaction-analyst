# Deploy to Agent Engine

source .venv/bin/activate

gcloud auth application-default login  

export GOOGLE_GENAI_USE_VERTEXAI=true
export GCP_PROJECT_ID="fsi-banking-agentspace"
export GCP_LOCATION="us-central1"
export GCP_STAGING_BUCKET="gs://fsi-banking-agentspace-adk-staging"

python deploy.py --create \
  --display-name "Transaction Analyst" \
  --description "An AI assistant for categorizing and evaluating financial transaction data."



# Update Agent Engine

source .venv/bin/activate

gcloud auth application-default login  

python deploy.py --list

export GOOGLE_GENAI_USE_VERTEXAI=true
export GCP_PROJECT_ID="fsi-banking-agentspace"
export GCP_LOCATION="us-central1"
export GCP_STAGING_BUCKET="gs://fsi-banking-agentspace-adk-staging"

python deploy.py --update \
  --agent-engine-id "projects/fsi-banking-agentspace/locations/us-central1/reasoningEngines/1486333012567130112" \
  --display-name "Transaction Analyst" \
  --description "An AI assistant for categorizing and evaluating financial transaction data."


  # Managing the Transaction Analyst Agent in Agentspace

# Register Agent

curl -X POST \
-H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application/json" \
-H "X-Goog-User-Project: fsi-banking-agentspace" \
"https://discoveryengine.googleapis.com/v1alpha/projects/fsi-banking-agentspace/locations/global/collections/default_collection/engines/keybank-agentspace_1748531908684/assistants/default_assistant/agents" \
-d '{
    "displayName": "Transaction Analyst",
    "description": "Provides in-depth financial market analysis, including trends, stock performance, and economic indicators.",
    "icon": {
        "uri": "https://storage.cloud.google.com/miscellaneous-demo-assets/market_analyst_icon.jpg"
    },
    "adk_agent_definition": {
        "tool_settings": {
            "tool_description": "You are an expert financial Transaction Analyst. Use your tools to retrieve and analyze data on stock prices, market trends, and economic indicators to answer user queries."
        },
        "provisioned_reasoning_engine": {
            "reasoning_engine": "projects/fsi-banking-agentspace/locations/us-central1/reasoningEngines/1486333012567130112"
        }
    }
}'

# Update Agent

curl -X PATCH \
-H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application/json" \
-H "X-Goog-User-Project: fsi-banking-agentspace" \
"https://discoveryengine.googleapis.com/v1alpha/projects/925334697476/locations/global/collections/default_collection/engines/equifax-agentspace_1755881391932/assistants/default_assistant/agents/1486333012567130112" \
-d '{
     "displayName": "Transaction Analyst",
     "description": "Provides in-depth financial market analysis, including trends, stock performance, and economic indicators.",
     "icon": {
          "uri": "https://storage.cloud.google.com/miscellaneous-demo-assets/market_analyst_icon.jpg"
     },
     "adk_agent_definition": {
          "tool_settings": {
               "tool_description": "You are an expert financial Transaction Analyst. Use your tools to retrieve and analyze data on stock prices, market trends, and economic indicators to answer user queries."
          },
          "provisioned_reasoning_engine": {
               "reasoning_engine": "projects/fsi-banking-agentspace/locations/us-central1/reasoningEngines/1486333012567130112"
          }
     }
}'

# Unregister Agent

# Replace AGENT_ID with the actual agent's ID
AGENT_ID="3784264572786442474"

curl -X DELETE \
-H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "X-Goog-User-Project: fsi-banking-agentspace" \
"https://discoveryengine.googleapis.com/v1alpha/projects/fsi-banking-agentspace/locations/global/collections/default_collection/engines/keybank-agentspace_1748531908684/assistants/default_assistant/agents/${AGENT_ID}"