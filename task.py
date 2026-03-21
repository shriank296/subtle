from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Optional

from brit_common.adapter.feature_flag.adapter import FeatureFlagAdapter
from brit_common.ports.storage import StorageAdapter
from pydantic import UUID4, BaseModel

from app.ports.db import Repositories


class TaskStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TaskStatusResponse(BaseModel):
    instance_id: str
    created_time: datetime
    last_updated_time: datetime
    runtime_status: str
    error_file: str | None = None


class PipelineParam(BaseModel):
    pipeline_name: str
    task_param: dict


class TaskConfig(BaseModel):
    bach_size: int = 0
    max_concurrency: int = 5


class TaskParam(BaseModel):
    task_name: str
    task_param: dict


class TaskReturn(BaseModel):
    status: TaskStatus
    result: dict | None = None
    next_task_param: list[TaskParam] | None = None


class TaskClientAdapter(ABC):
    """
    Provides data from DEV TASK, can be extended to get different values as needed.
    Pulls auth credentials from env vars that should be stored in key vault, example usage:

    adapter = TaskClientAdapter()
    adapter.get_status().json()

    """

    @abstractmethod
    def get_status(self, file_id: UUID4) -> TaskStatusResponse:
        pass


class TaskAdapter(ABC):
    def __init__(
        self,
        repositories: Repositories,
        storage: StorageAdapter,
        feature_flags: FeatureFlagAdapter,
    ):
        self.repositories: Repositories = repositories
        self.storage: StorageAdapter = storage
        self.feature_flags: FeatureFlagAdapter = feature_flags

    @abstractmethod
    def run(self, task_param: dict, **kwargs) -> TaskReturn:
        pass
