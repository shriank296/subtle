import importlib
import logging
from abc import abstractmethod
from typing import Dict, List, Type

from brit_common.adapter.feature_flag.adapter import FeatureFlagAdapter
from brit_common.ports.storage import StorageAdapter

from app.ports.db import Repositories
from app.ports.task import AbstractPipeline, AbstractPipelineAdapter, TaskReturn

logger = logging.getLogger(__name__)


class PipelineNotFound(Exception):
    pass


class TaskRouterException(Exception):
    pass


def run_task(task_name, task_param) -> TaskReturn:
    try:
        handler = importlib.import_module(f"app.adapter.task.tasks.{task_name}")
        task_result: TaskReturn = handler.handle(task_param)
        logger.info(f"Task: {task_name}, result: {task_result.status}")
    except ImportError as err:
        raise TaskRouterException(f"Task not found: {task_name} with error: {str(err)}")
    return task_result


class BasePipelineAdapter(AbstractPipelineAdapter):
    def __init__(
        self,
        repositories: Repositories,
        storage: StorageAdapter,
        feature_flags: FeatureFlagAdapter,
    ) -> None:
        self.repositories: Repositories = repositories
        self.storage: StorageAdapter = storage
        self.feature_flags: FeatureFlagAdapter = feature_flags

    def call_task(self, task_name: str, task_param: dict) -> dict:
        task_return = run_task(task_name, task_param)
        return task_return.model_dump()

    def call_multiple_tasks(self, task_names: list[str], task_params: list[dict]):
        results = []
        for task, params in zip(task_names, task_params):
            results.append(self.call_task(task, params))
        return results


class BasePipeline(AbstractPipeline):
    pipelines: dict[str, type["BasePipeline"]] = {}

    def __init__(self, pipeline_adapter: BasePipelineAdapter):
        self.pipeline_adapter: BasePipelineAdapter = pipeline_adapter
        self.results: list[dict] = []

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.pipelines[cls.__name__] = cls

    @classmethod
    def get_pipeline(cls, name: str) -> type[AbstractPipeline]:
        if name not in cls.pipelines:
            err_message = (
                f"Pipeline not found, available pipelines are: {cls.pipelines.keys()}"
            )
            logger.error(err_message)
            raise PipelineNotFound(err_message)

        return cls.pipelines[name]

    def call_task(self, task, task_param):
        return self.pipeline_adapter.call_task(task.task_name, task_param)

    def call_multiple_tasks(self, tasks, task_params):
        names = [task.task_name for task in tasks]
        return self.pipeline_adapter.call_multiple_tasks(names, task_params)

    def last_task_successful(self) -> bool:
        last_result = self.results[-1]
        if isinstance(last_result, list):
            task_status = [result["status"] for result in last_result]
            return all([status == "SUCCESS" for status in task_status])
        else:
            return last_result["status"] == "SUCCESS"

    @abstractmethod
    def run_pipeline(self, pipeline_param):
        pass
