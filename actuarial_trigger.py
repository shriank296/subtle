import json
import logging

import azure.durable_functions as df  # type: ignore
import azure.functions as func

from app.domain.actuarial_rater.dto import ActuarialPipelineParam

logger = logging.getLogger(__name__)


async def handle(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    client = df.DurableOrchestrationClient(starter)

    pipeline_params: ActuarialPipelineParam = ActuarialPipelineParam(
        pipeline_name="ActuarialPricingPipeline",
        task_param=req.params,
    )

    instance_id = await client.start_new(
        "az_durable_func_orchestrator", None, pipeline_params.model_dump()
    )

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

    return func.HttpResponse(
        body=json.dumps({"instance_id": instance_id}),
        status_code=200,
    )
