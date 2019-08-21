import asyncio
import logging
from pathlib import Path
from threading import Thread
from typing import Optional, Dict, Any, Tuple, List, Awaitable, Callable

from dataclasses import dataclass, asdict, field
from golem_task_api import RequestorAppHandler, ProviderAppHandler, entrypoint
from golem_task_api.server import AppServer
from golem_task_api.structs import Subtask
from twisted.internet import defer, threads

from golem.envs import (
    CounterId,
    CounterUsage,
    EnvConfig,
    EnvId,
    EnvironmentBase,
    EnvMetadata,
    EnvSupportStatus,
    Prerequisites,
    Runtime,
    RuntimeBase,
    RuntimeInput,
    RuntimeOutput,
    RuntimePayload
)
from golem.task.task_api import TaskApiPayloadBuilder

logger = logging.getLogger(__name__)


class LocalhostConfig(EnvConfig):

    def to_dict(self) -> dict:
        return {}

    @staticmethod
    def from_dict(data: dict) -> 'LocalhostConfig':
        return LocalhostConfig()


async def _not_implemented(*_):
    raise NotImplementedError


@dataclass
class LocalhostPrerequisites(Prerequisites):
    compute: Callable[[str, dict], Awaitable[str]] = _not_implemented
    run_benchmark: Callable[[], Awaitable[float]] = _not_implemented
    next_subtask: Callable[[], Awaitable[Subtask]] = _not_implemented
    has_pending_subtasks: Callable[[], Awaitable[bool]] = _not_implemented
    verify: Callable[[str], Awaitable[Tuple[bool, Optional[str]]]] = \
        _not_implemented

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> 'LocalhostPrerequisites':
        return LocalhostPrerequisites(**data)


@dataclass
class LocalhostPayload(RuntimePayload):
    command: str
    shared_dir: Path
    prerequisites: LocalhostPrerequisites


class LocalhostPayloadBuilder(TaskApiPayloadBuilder):

    @classmethod
    def create_payload(
            cls,
            prereq: Prerequisites,
            shared_dir: Path,
            command: str,
            port: int
    ) -> RuntimePayload:
        assert isinstance(prereq, LocalhostPrerequisites)
        return LocalhostPayload(
            command=command,
            shared_dir=shared_dir,
            prerequisites=prereq
        )


class LocalhostAppHandler(RequestorAppHandler, ProviderAppHandler):

    def __init__(self, prereq: LocalhostPrerequisites) -> None:
        self._compute = prereq.compute
        self._benchmark = prereq.run_benchmark
        self._next_subtask = prereq.next_subtask
        self._has_pending_subtasks = prereq.has_pending_subtasks
        self._verify = prereq.verify

    async def create_task(
            self,
            task_work_dir: Path,
            max_subtasks_count: int,
            task_params: dict
    ) -> None:
        pass

    async def next_subtask(self, task_work_dir: Path) -> Subtask:
        return await self._next_subtask()

    async def verify(
            self,
            task_work_dir: Path,
            subtask_id: str
    ) -> Tuple[bool, Optional[str]]:
        return await self._verify(subtask_id)

    async def discard_subtasks(
            self,
            task_work_dir: Path,
            subtask_ids: List[str]
    ) -> List[str]:
        return []

    async def run_benchmark(self, work_dir: Path) -> float:
        return await self._benchmark()

    async def has_pending_subtasks(self, task_work_dir: Path) -> bool:
        return await self._has_pending_subtasks()

    async def compute(
            self,
            task_work_dir: Path,
            subtask_id: str,
            subtask_params: dict
    ) -> str:
        return await self._compute(subtask_id, subtask_params)


class LocalhostRuntime(RuntimeBase):

    def __init__(
            self,
            payload: LocalhostPayload,
    ) -> None:
        super().__init__(logger)
        self._command = payload.command
        self._work_dir = payload.shared_dir
        self._app_handler = LocalhostAppHandler(payload.prerequisites)

        self._server: Optional[AppServer] = None
        self._server_loop: Optional[asyncio.AbstractEventLoop] = None
        self._server_thread: Optional[Thread] = None

    def prepare(self) -> defer.Deferred:
        self._prepared()
        return defer.succeed(None)

    def clean_up(self) -> defer.Deferred:
        self._torn_down()
        return defer.succeed(None)

    def _spawn_server(self) -> None:
        asyncio.set_event_loop(self._server_loop)
        try:
            self._server_loop.run_until_complete(entrypoint(
                work_dir=self._work_dir,
                argv=self._command.split(),
                requestor_handler=self._app_handler,
                provider_handler=self._app_handler
            ))
        except Exception as e:  # pylint: disable=broad-except
            self._error_occurred(e, str(e))
        else:
            self._stopped()

    def start(self) -> defer.Deferred:
        self._server_loop = asyncio.new_event_loop()
        self._server_thread = Thread(target=self._spawn_server, daemon=False)
        self._server_thread.start()
        self._started()
        return defer.succeed(None)

    def stop(self) -> defer.Deferred:
        assert self._server_loop is not None
        assert self._server_thread is not None
        try:
            self._server_loop.call_soon_threadsafe(self._server_loop.stop)
            self._server_loop.call_soon_threadsafe(self._server_loop.close)
        except Exception:  # pylint: disable=broad-except
            return defer.fail()

        return threads.deferToThread(self._server_thread.join, timeout=10)

    def stdin(self, encoding: Optional[str] = None) -> RuntimeInput:
        raise NotImplementedError

    def stdout(self, encoding: Optional[str] = None) -> RuntimeOutput:
        raise NotImplementedError

    def stderr(self, encoding: Optional[str] = None) -> RuntimeOutput:
        raise NotImplementedError

    def get_port_mapping(self, port: int) -> Tuple[str, int]:
        return '127.0.0.1', port

    def usage_counters(self) -> Dict[CounterId, CounterUsage]:
        return {}

    def call(self, alias: str, *args, **kwargs) -> defer.Deferred:
        raise NotImplementedError


class LocalhostEnvironment(EnvironmentBase):

    """ This environment is capable of spawning Task API services on localhost.
    Spawned services provide stub implementations of Task API methods returning
    values specified in prerequisites. """

    def __init__(
            self,
            config: LocalhostConfig,
            env_id: EnvId = 'localhost'
    ) -> None:
        super().__init__(logger)
        self._config = config
        self._env_id = env_id

    @classmethod
    def supported(cls) -> EnvSupportStatus:
        return EnvSupportStatus(supported=True)

    def prepare(self) -> defer.Deferred:
        self._env_enabled()
        return defer.succeed(None)

    def clean_up(self) -> defer.Deferred:
        self._env_disabled()
        return defer.succeed(None)

    def run_benchmark(self) -> defer.Deferred:
        return defer.succeed(1.0)

    def metadata(self) -> EnvMetadata:
        return EnvMetadata(
            id=self._env_id,
            description='Localhost environment',
            supported_counters=[],
            custom_metadata={}
        )

    @classmethod
    def parse_prerequisites(
            cls,
            prerequisites_dict: Dict[str, Any]
    ) -> Prerequisites:
        return LocalhostPrerequisites.from_dict(prerequisites_dict)

    def install_prerequisites(
            self,
            prerequisites: Prerequisites
    ) -> defer.Deferred:
        self._prerequisites_installed(prerequisites)
        return defer.succeed(True)

    @classmethod
    def parse_config(cls, config_dict: Dict[str, Any]) -> EnvConfig:
        return LocalhostConfig.from_dict(config_dict)

    def config(self) -> EnvConfig:
        return self._config

    def update_config(self, config: EnvConfig) -> None:
        self._config = config
        self._config_updated(config)

    def runtime(
            self,
            payload: RuntimePayload,
            config: Optional[EnvConfig] = None
    ) -> Runtime:
        assert isinstance(payload, LocalhostPayload)
        return LocalhostRuntime(payload)
