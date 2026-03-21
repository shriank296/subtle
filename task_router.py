# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
import logging
from typing import Dict

from app.ports.task import TaskParam, TaskReturn

logger = logging.getLogger(__name__)

from app.adapter.task.pipeline import run_task
from app.domain.actuarial_rater.task import *
from app.domain.example.task import *
from app.domain.valuation.task import *

from ..tracing import simple_tracing


def task_router(task_data: dict) -> dict:
    with simple_tracing():
        task_param: TaskParam = TaskParam(**task_data)
        task_return: TaskReturn = run_task(task_param.task_name, task_param.task_param)
        return task_return.model_dump()
