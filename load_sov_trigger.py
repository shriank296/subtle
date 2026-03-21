import json
import logging

import azure.durable_functions as df  # type: ignore
import azure.functions as func

from app.config import AZURE_INGESTION_DIR, ENVIRONMENT
from app.domain.valuation.dto import IngestDataParam
from app.ports.task import PipelineParam

logger = logging.getLogger(__name__)


async def handle(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    quote_id = req.params["quote_id"]
    file_id = req.params["file_id"]

    file_name = (
        file_url
    ) = f"{ENVIRONMENT}/{AZURE_INGESTION_DIR}/{quote_id}/{file_id}/ImportedData.json"

    pipeline_params: PipelineParam = PipelineParam(
        pipeline_name="LoadSovPipeline",
        task_param=IngestDataParam(file_name=file_name, file_url=file_url).model_dump(),
    )

    instance_id = await client.start_new(
        "az_durable_func_orchestrator", None, pipeline_params.model_dump()
    )

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return func.HttpResponse(
        body=json.dumps({"instance_id": instance_id}),
        status_code=200,
    )
