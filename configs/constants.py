import os
from dotenv import load_dotenv

# load env variables from .env if any
load_dotenv()

# Azure Cloud 
TENANT_ID = os.environ.get("TENANT_ID", " ")
INTERACTIVE_CLIENT_ID = os.environ.get("INTERACTIVE_CLIENT_ID", "xxx")
AZURE_LOGIN_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE_INTERACTIVE_CLI = f"api://{INTERACTIVE_CLIENT_ID}/.default"
SCOPE_INTERACTIVE_BROWSER = f"api://{INTERACTIVE_CLIENT_ID}/token"

# Client secrets
NON_INTERACTIVE_CLIENT_ID = os.environ.get("INTERACTIVE_CLIENT_ID", "xxx")
SERVICE_PRINCIPAL = os.environ.get("SERVICE_PRINCIPAL", "xxx")
SERVICE_PRINCIPAL_SECRET = os.environ.get("SERVICE_PRINCIPAL_SECRET", "xxx")
SCOPE_NON_INTERACTIVE = f"api://{NON_INTERACTIVE_CLIENT_ID}/.default"

# OpenAI
OPENAI_LOG = os.environ.get("OPENAI_LOG", "info")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "xxx")
OPENAI_API_TYPE = os.environ.get("OPENAI_API_TYPE", "azure")
OPENAI_API_VERSION = os.environ.get("OPENAI_API_VERSION", "2023-03-15-preview")
OPENAI_API_BASE = os.environ.get("OPENAI_API_BASE", "https://xxx/cse/prod/proxy")

OPENAI_ACCOUNT_NAME = "xxx"
AZURE_SUBSCRIPTION_ID = "xxx"
AZURE_RG_NAME = "xxx"
