#!/usr/bin/env python3
# Fabric notebook source
# METADATA ********************
# META {
# META   "kernel_info": {  
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }
# CELL ********************   

import os
import sys
import time
import msal
import csv
from azure.core.credentials import AccessToken
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
from pathlib import Path

# ---------- Utilities ----------
def get_env(name: str) -> str: 
    v = os.getenv(name)
    if not v or not v.strip():
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return v.strip()

def get_workspace_id_from_csv(env: str, csv_path: str) -> str:
    with open(csv_path, newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        # normalize field names once
        norm_fields = [h.strip().lower() for h in r.fieldnames]
        for row in r:
            # build a normalized-row dict (keys lower/stripped)
            nrow = {k.strip().lower(): v.strip() for k, v in row.items()}  # [1][2]
            if nrow.get("environment","").lower() == env.strip().lower():
                ws = nrow.get("workspace id","")
                if not ws:
                    raise ValueError(f"workspace id missing for env={env} in {csv_path}")
                return ws
    raise ValueError(f"No row for env={env} in {csv_path}")

class StaticTokenCredential:
    """
    Minimal azure.core TokenCredential wrapper returning a static bearer token.
    Use when a library expects 'credential=' rather than a raw token string.
    """
    def __init__(self, token: str, expires_in: int = 3500):
        self._token = token
        self._exp = int(time.time()) + expires_in
    def get_token(self, *scopes, **kwargs) -> AccessToken:
        return AccessToken(self._token, self._exp)

def get_access_token(tenant_id: str, client_id: str, client_secret: str, auth_host: str | None = None) -> str:
    """
    Client Credentials flow for Microsoft Fabric using MSAL.
    Uses the .default scope so granted application permissions apply.
    """
    auth_host = (auth_host or "https://login.microsoftonline.com").rstrip("/")
    authority_url = f"{auth_host}/{tenant_id}"
    scopes = ["https://api.fabric.microsoft.com/.default"]

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority_url,
        client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in token_response:
        return token_response["access_token"]
    raise RuntimeError(f"Failed to acquire token: {token_response}")


def main(env: str):
    print(f"🚀 Deploying to environment: {env}")

    TENANT_ID = get_env("AZURE_TENANT_ID")
    CLIENT_ID = get_env("AZURE_CLIENT_ID")
    CLIENT_SECRET = get_env("AZURE_CLIENT_SECRET")
    WORKSPACE_ID = get_env("FABRIC_WORKSPACE_ID")

    os.environ["AZURE_TENANT_ID"] = TENANT_ID
    os.environ["AZURE_CLIENT_ID"] = CLIENT_ID
    os.environ["AZURE_CLIENT_SECRET"] = CLIENT_SECRET

    AUTH_HOST = "https://login.microsoftonline.com"

    here = Path(__file__).resolve().parent                        
    csv_file = here.parent / "config" / "workspace_data.csv" 
    WORKSPACE_ID = get_workspace_id_from_csv(env, csv_file)

    access_token = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET, AUTH_HOST)
    token_cred = StaticTokenCredential(access_token)
    repository_directory = os.getcwd()

    try:
        target_workspace = FabricWorkspace(
            workspace_id=WORKSPACE_ID,
            repository_directory=repository_directory,
            item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
            credential=token_cred
        )
    except TypeError:
        target_workspace = FabricWorkspace(
            workspace_id=WORKSPACE_ID,
            repository_directory=repository_directory,
            item_type_in_scope=["Notebook", "DataPipeline", "Environment"],
            access_token=access_token
        )

    publish_all_items(target_workspace)
    unpublish_all_orphan_items(target_workspace)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        env = sys.argv[1]  
    else:
        env = "dev"  
    main(env)


# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }




