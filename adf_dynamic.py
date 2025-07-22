import requests
from azure.identity import DefaultAzureCredential
from config import SUBSCRIPTION_ID, RESOURCE_GROUP, ADF_NAME

# def generate_pipeline_json(pipeline_name, parameters):
#     return {
#         "name": pipeline_name,
#         "properties": {
#             "description": f"User-defined pipeline {pipeline_name}",
#             "activities": [
#                 {
#                     "name": "LogParameters",
#                     "type": "WebActivity",
#                     "typeProperties": {
#                         "url": "https://postman-echo.com/post",
#                         "method": "POST",
#                         "headers": {"Content-Type": "application/json"},
#                         "body": str(parameters)
#                     }
#                 }
#             ],
#             "parameters": {
#                 k: {"type": "String"} for k in parameters
#             }
#         }
#     }

def generate_pipeline_json(pipeline_name, parameters):
    return {
        "name": pipeline_name,
        "properties": {
            "description": f"User-defined pipeline {pipeline_name}",
            "activities": [
                {
                    "name": "RunDatabricksNotebook",
                    "type": "DatabricksNotebook",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "7.00:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False
                    },
                    "userProperties": [],
                    "typeProperties": {
                        "notebookPath": "/Workspace/Data_Cleaning",
                        "baseParameters": parameters
                    },
                    "linkedServiceName": {
                        "referenceName": "AzureDatabricks1",
                        "type": "LinkedServiceReference"
                    }
                }
            ],
            "parameters": {
                k: {"type": "String"} for k in parameters
            }
        }
    }


def create_pipeline(pipeline_name, parameters):
    token = DefaultAzureCredential().get_token("https://management.azure.com/.default").token
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/{ADF_NAME}/pipelines/{pipeline_name}?api-version=2018-06-01"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    pipeline_def = generate_pipeline_json(pipeline_name, parameters)

    response = requests.put(url, headers=headers, json=pipeline_def)
    return response.status_code == 200 or response.status_code == 201

def trigger_pipeline(pipeline_name, parameters):
    token = DefaultAzureCredential().get_token("https://management.azure.com/.default").token
    url = f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory/factories/{ADF_NAME}/pipelines/{pipeline_name}/createRun?api-version=2018-06-01"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json={"parameters": parameters})
    return response.json().get("runId", None)
