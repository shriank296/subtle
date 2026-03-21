import logging
import os
import time
import uuid
from abc import abstractmethod
from contextlib import contextmanager
from cProfile import Profile
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, Type

from brit_common.adapter.auth.oauth2 import HttpRequest
from brit_common.adapter.feature_flag.adapter import FeatureFlagAdapter
from brit_common.ports.storage import StorageAdapter
from pydantic import BaseModel, ValidationError

from app.adapter.error_report import ErrorReporting
from app.adapter.file.json_data_factory import JsonDataFactory
from app.adapter.task.client import FileNotFound
from app.config import AZURE_INGESTION_DIR, ERROR_FOLDER
from app.domain.valuation.logic import get_file_id, get_quote_id
from app.ports.db import Repositories
from app.ports.task import TaskAdapter, TaskReturn

logger = logging.getLogger(__name__)


class BaseTaskAdapter(TaskAdapter):
    task_name: str
    tasks: dict[str, type["TaskAdapter"]] = {}
    task_param_DTO: type[BaseModel] | None = None

    def __init__(
        self,
        repositories: Repositories,
        storage: StorageAdapter,
        feature_flags: FeatureFlagAdapter,
        client: HttpRequest | None = None,
    ):
        self.repositories: Repositories = repositories
        self.storage: StorageAdapter = storage
        self.feature_flags: FeatureFlagAdapter = feature_flags
        self.task_param: BaseModel
        self.client = client
        self.enable_profiling = feature_flags.get_flag("FF_ENABLE_PROFILING")

    def get_quote_file_id(self, task_param) -> tuple[str | None, str | None]:
        quote_id = None
        file_id = None
        try:
            quote_id = str(get_quote_id(task_param=task_param))
            file_id = str(get_file_id(task_param=task_param))
        except:
            pass
        return quote_id, file_id

    def get_error_file_path(self, task_param: Any) -> str:
        """
        Construct the full SOV error file path.
        """
        quote_id, file_id = self.get_quote_file_id(task_param)
        if quote_id and file_id:
            return f"{self.storage.container_name}/{AZURE_INGESTION_DIR}/{quote_id}/{file_id}/{ERROR_FOLDER}/{self.__class__.__name__}_{time.time()}.txt"
        return f"{self.storage.container_name}/{AZURE_INGESTION_DIR}/{ERROR_FOLDER}/{self.__class__.__name__}_{time.time()}.txt"

    def init_error_reporting(self, **kwargs) -> ErrorReporting:
        error_file_path = self.get_error_file_path(self.task_param)
        return ErrorReporting(error_file_path, task_name=self.__class__.__name__)

    def validate_params(self, task_param: dict):
        if task_param:
            assert self.task_param_DTO, "Task missing param DTO definition"
            try:
                self.task_param = self.task_param_DTO(**task_param)
            except ValidationError as err:
                logger.error(
                    f"Unable to run task: {self.__class__.__name__} as invalid task params passed: {err}"
                )
                raise

    def run(self, task_param: dict, **kwargs) -> TaskReturn:
        self.validate_params(task_param=task_param)
        error_report = self.init_error_reporting()
        with error_report.recording():
            with self.logging(task_param=task_param):
                with self.repositories.db.transaction():  # type: ignore
                    if self.enable_profiling:
                        os.makedirs("profiling/", exist_ok=True)
                        timestamp = str(datetime.now().time())[0:5].replace(":", "")
                        with Profile() as profile_task:
                            self._setup(**kwargs)
                            task_return: TaskReturn = self._run(**kwargs)
                            self._output(**kwargs)
                            profile_task.dump_stats(
                                f"profiling/{timestamp}_{self.task_name}.perf"
                            )
                    else:
                        self._setup(**kwargs)
                        task_return = self._run(**kwargs)
                        self._output(**kwargs)
                    assert isinstance(
                        task_return, TaskReturn
                    ), f"This task is not returning the correct type {self.__class__.__name__}"

                    return task_return

    @contextmanager
    def logging(self, task_param: Any):
        start_time = time.time()
        logging.info(
            f"Starting Task: {self.__class__.__name__} with params: {str(task_param)}"
        )
        yield
        end_time = time.time()
        total_time = end_time - start_time
        logging.info(
            f"Completed Task: {self.__class__.__name__} with params: {str(task_param)} "
            + " in: "
            + str(total_time)
            + " seconds"
        )

    def _setup(self):
        pass

    @abstractmethod
    def _run(self, **kwargs) -> TaskReturn:
        pass

    def _output(self):
        pass
