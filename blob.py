import logging
from typing import Tuple

import azure.durable_functions as df  # type: ignore
import azure.functions as func
from brit_common.adapter.storage import AzureStorageAdapter

from app.config import AZURE_STORAGE_CONNECTION_STRING, ENVIRONMENT
from app.domain.valuation.dto import IngestDataParam
from app.ports.task import PipelineParam

from ..tracing import simple_tracing

logger = logging.getLogger(__name__)


def move_ingestion_file(
    ingestion_file_name: str, ingestion_file_url: str
) -> tuple[str, str]:
    storage = AzureStorageAdapter(
        {
            "container_name": "ingestion",
            "connection_string": AZURE_STORAGE_CONNECTION_STRING,
        }
    )

    # Copy file to main storage
    logging.info(
        f"Moving ingestion file: '{ingestion_file_name}' from ingestion to env folder"
    )

    ingestion_file = storage.load(str(ingestion_file_name))
    env_file_name = str(ingestion_file_name).replace("ingestion", ENVIRONMENT)
    env_file_url = str(ingestion_file_url).replace("ingestion", ENVIRONMENT)
    storage.save(env_file_name, ingestion_file.read())

    logging.info(f"Deleting ingestion file: '{ingestion_file_name}'")

    # Remove ingestion file
    storage.delete(str(ingestion_file_name))

    return env_file_name, env_file_url


async def handle_file(blob_file: func.InputStream, starter: str):
    with simple_tracing():
        client = df.DurableOrchestrationClient(starter)

        file_name, file_url = move_ingestion_file(
            str(blob_file.name), str(blob_file.uri)
        )

        df_param: PipelineParam = PipelineParam(
            pipeline_name="LoadSovPipeline",
            task_param=IngestDataParam(
                file_name=file_name, file_url=file_url
            ).model_dump(),
        )

        logging.info(f"Processing blob '{file_name}': '{file_url}'")

        if str(blob_file.name).endswith("ImportedData.json"):
            instance_id = await client.start_new(
                "az_durable_func_orchestrator", None, df_param.model_dump()
            )
            logging.info(f"Started orchestration with ID = '{instance_id}'.")
        else:
            logging.info(f"Skipping unknown file: {file_name}")

    return
