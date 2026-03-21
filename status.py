# This function an HTTP starter function for Durable Functions.

import datetime
import json
from typing import Dict, List

import azure.durable_functions as df  # type: ignore
import azure.functions as func
from azure.data.tables import TableEntity  # type: ignore
from azure.data.tables import TableClient, TableServiceClient
from azure.durable_functions.models.DurableOrchestrationStatus import (  # type: ignore
    DurableOrchestrationStatus,
)

from app.config import (
    AZURE_DURABLE_FUNCTION_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_TABLE_NAME,
)


def get_table_client() -> TableClient:
    """
    Get Azure table client for interacting with azure data tables
    """
    table_name = f"{AZURE_STORAGE_TABLE_NAME}Instances"
    table_service_client = TableServiceClient.from_connection_string(
        conn_str=AZURE_DURABLE_FUNCTION_STORAGE_CONNECTION_STRING
    )
    table_client: TableClient = table_service_client.get_table_client(
        table_name=table_name
    )
    return table_client


def get_instances_with_file_id(file_id: str) -> list[TableEntity]:
    """
    Access azure data table to look for instances with file id.

    This replaces durable function client logic, at time of writing
    the durable function client only supports basic filters and has a max of
    100 results so it's difficult to get what we need from that.
    """
    table_client: TableClient = get_table_client()
    entities = table_client.list_entities()
    instances = []
    for entity in entities:
        if file_id in entity.get("Input", ""):
            instances.append(entity)
    return instances


async def handle(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)
    url_param = req.params

    def create_status_obj(instance: DurableOrchestrationStatus) -> object:
        return {
            "name": instance.name,
            "instanceId": instance.instance_id,
            "createdTime": str(instance.created_time),
            "lastUpdatedTime": str(instance.last_updated_time),
            "output": instance.output,
            "taskName": get_task_name(instance.input_),
            "runtimeStatus": instance.runtime_status.value,
            "input": instance.input_ if instance.runtime_status else None,
        }

    def get_task_name(_input) -> str:
        if _input:
            try:
                return json.loads(_input).get("task_name", None)
            except json.decoder.JSONDecodeError:
                pass
        return "Unknown"

    if "instance_id" in url_param:
        return client.create_check_status_response(req, url_param["instance_id"])

    elif "task_id" in url_param:
        task_id = url_param["task_id"]
        instance: DurableOrchestrationStatus = await client.get_status(
            task_id.replace("-", "")
        )
        if not instance.instance_id:
            return func.HttpResponse(
                body=json.dumps({"error": f"Task not found: {task_id}"}),
                status_code=404,
            )

        instance_obj: object = create_status_obj(instance)
        return func.HttpResponse(body=json.dumps(instance_obj), status_code=200)

    elif "file_id" in url_param:
        file_id = url_param["file_id"]
        instance_records: list[TableEntity] = get_instances_with_file_id(file_id)
        instance_file_data = []

        for i in instance_records:
            instance_file_data.append(
                {
                    "instanceId": i["ExecutionId"],
                    "createdTime": str(i["CreatedTime"]),
                    "lastUpdatedTime": str(i["LastUpdatedTime"]),
                    "runtimeStatus": i["RuntimeStatus"],
                }
            )

        sorted_instance_data = sorted(
            instance_file_data, key=lambda x: x["lastUpdatedTime"], reverse=True
        )

        return func.HttpResponse(body=json.dumps(sorted_instance_data), status_code=200)

    else:
        orchestration_instances: list[
            DurableOrchestrationStatus
        ] = await client.get_status_all()
        instance_data: list[DurableOrchestrationStatus] = [
            create_status_obj(i) for i in orchestration_instances
        ]

        return func.HttpResponse(body=json.dumps(instance_data), status_code=200)
