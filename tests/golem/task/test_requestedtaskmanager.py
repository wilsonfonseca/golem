import asyncio

from golem_task_api.client import RequestorAppClient
from golem_task_api.structs import Subtask
from mock import Mock, patch
import pytest

from golem.model import default_now, RequestedTask, RequestedSubtask
from golem.task.envmanager import EnvironmentManager
from golem.task.requestedtaskmanager import (
    ComputingNode,
    CreateTaskParams,
    RequestedTaskManager,
    ComputingNodeDefenition,
)
from golem.task.taskstate import TaskStatus, SubtaskStatus
from golem.testutils import AsyncDatabaseFixture


class TestRequestedTaskManager(AsyncDatabaseFixture):

    def setup_method(self, method):
        super().setup_method(method)
        self.env_manager = Mock(spec=EnvironmentManager)
        self.public_key = str.encode('0xdeadbeef')
        self.rtm = RequestedTaskManager(
            env_manager=self.env_manager,
            public_key=self.public_key,
            root_path=self.new_path
        )

    def teardown_method(self, *args, **kwargs):
        RequestedSubtask.delete().execute()
        RequestedTask.delete().execute()
        super().teardown_method(*args, **kwargs)

    def test_create_task(self):
        # given
        golem_params = self._build_golem_params()
        app_params = {}
        # when
        task_id = self.rtm.create_task(golem_params, app_params)
        # then
        row = RequestedTask.get(RequestedTask.task_id == task_id)
        assert row.status == TaskStatus.creating
        assert row.start_time < default_now()

    @pytest.mark.asyncio
    async def test_init_task(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)

        task_id = self._create_task()
        # when
        await self.rtm.init_task(task_id)
        row = RequestedTask.get(RequestedTask.task_id == task_id)
        # then
        assert row.status == TaskStatus.creating
        assert mock_client.create_task.called_once_with(
            row.task_id,
            row.max_subtasks,
            row.app_params
        )
        self.env_manager.enabled.assert_called_once_with(row.environment)

    @pytest.mark.asyncio
    async def test_init_task_wrong_status(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)

        task_id = self._create_task()
        # when
        await self.rtm.init_task(task_id)
        # Start task to change the status
        self.rtm.start_task(task_id)
        # then
        with pytest.raises(RuntimeError):
            await self.rtm.init_task(task_id)

    @pytest.mark.asyncio
    async def test_start_task(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)

        task_id = self._create_task()
        await self.rtm.init_task(task_id)
        # when
        self.rtm.start_task(task_id)
        # then
        row = RequestedTask.get(RequestedTask.task_id == task_id)
        assert row.status == TaskStatus.waiting

    def test_task_exists(self):
        task_id = self._create_task()
        self.assertTrue(self.rtm.task_exists(task_id))

    def test_task_not_exists(self):
        task_id = 'a'
        self.assertFalse(self.rtm.task_exists(task_id))

    @pytest.mark.asyncio
    async def test_has_pending_subtasks(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)
        self._add_has_pending_subtasks_to_client_mock(mock_client)

        task_id = self._create_task()
        await self.rtm.init_task(task_id)
        self.rtm.start_task(task_id)
        # when
        res = await self.rtm.has_pending_subtasks(task_id)
        # then
        self.assertTrue(res)
        mock_client.has_pending_subtasks.assert_called_once_with(task_id)

    @pytest.mark.asyncio
    async def test_get_next_subtask(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)
        self._add_next_subtask_to_client_mock(mock_client)

        task_id = self._create_task()
        await self.rtm.init_task(task_id)
        self.rtm.start_task(task_id)
        computing_node = ComputingNode.create(
            node_id='abc',
            name='abc',
        )
        # when
        res = await self.rtm.get_next_subtask(task_id, computing_node)

        row = RequestedSubtask.get(
            RequestedSubtask.subtask_id == res.subtask_id)
        # then
        self.assertEqual(row.task_id, task_id)
        self.assertEqual(row.computing_node, computing_node)
        mock_client.next_subtask.assert_called_once_with(task_id)

    @pytest.mark.asyncio
    async def test_verify(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)
        self._add_next_subtask_to_client_mock(mock_client)
        self._add_verify_to_client_mock(mock_client)

        task_id = self._create_task()
        await self.rtm.init_task(task_id)
        self.rtm.start_task(task_id)
        computing_node = ComputingNode.create(
            node_id='abc',
            name='abc',
        )
        subtask = await self.rtm.get_next_subtask(task_id, computing_node)

        # The second call should return false so the client will shut down
        self._add_has_pending_subtasks_to_client_mock(mock_client, False)
        subtask_id = subtask.subtask_id
        # when
        res = await self.rtm.verify(task_id, subtask.subtask_id)

        task_row = RequestedTask.get(RequestedTask.task_id == task_id)
        subtask_row = RequestedSubtask.get(
            RequestedSubtask.subtask_id == subtask_id)
        # then
        self.assertTrue(res)
        mock_client.verify.assert_called_once_with(task_id, subtask.subtask_id)
        mock_client.shutdown.assert_called_once_with()
        self.assertTrue(task_row.status.is_completed())
        self.assertTrue(subtask_row.status.is_finished())

    @pytest.mark.asyncio
    async def test_verify_failed(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)
        self._add_next_subtask_to_client_mock(mock_client)
        self._add_verify_to_client_mock(mock_client, False)

        task_id = self._create_task()
        await self.rtm.init_task(task_id)
        self.rtm.start_task(task_id)
        computing_node = ComputingNodeDefenition(
            node_id='abc',
            name='abc',
        )
        subtask = await self.rtm.get_next_subtask(task_id, computing_node)

        subtask_id = subtask.subtask_id
        # when
        res = await self.rtm.verify(task_id, subtask.subtask_id)

        task_row = RequestedTask.get(RequestedTask.task_id == task_id)
        subtask_row = RequestedSubtask.get(
            RequestedSubtask.subtask_id == subtask_id)
        # then
        self.assertFalse(res)
        mock_client.verify.assert_called_once_with(task_id, subtask.subtask_id)
        mock_client.shutdown.assert_not_called()
        self.assertTrue(task_row.status.is_active())
        self.assertEqual(subtask_row.status, SubtaskStatus.failure)

    @pytest.mark.asyncio
    async def test_abort(self):
        # given
        mock_client = self._mock_client_create()
        self._add_create_task_to_client_mock(mock_client)
        self._add_next_subtask_to_client_mock(mock_client)

        task_id = self._create_task()
        await self.rtm.init_task(task_id)
        self.rtm.start_task(task_id)
        computing_node = ComputingNodeDefenition(
            node_id='abc',
            name='abc',
        )
        subtask = await self.rtm.get_next_subtask(task_id, computing_node)

        subtask_id = subtask.subtask_id
        # when
        await self.rtm.abort_task(task_id)
        task_row = RequestedTask.get(RequestedTask.task_id == task_id)
        subtask_row = RequestedSubtask.get(
            RequestedSubtask.subtask_id == subtask_id)
        # then
        mock_client.shutdown.assert_called_once_with()
        assert task_row.status == TaskStatus.aborted
        assert subtask_row.status == SubtaskStatus.cancelled

    def _build_golem_params(self) -> CreateTaskParams:
        return CreateTaskParams(
            app_id='a',
            name='a',
            environment='a',
            task_timeout=1,
            subtask_timeout=1,
            output_directory=self.new_path / 'output',
            resources=[],
            max_subtasks=1,
            max_price_per_hour=1,
            concent_enabled=False,
        )

    def _create_task(self):
        golem_params = self._build_golem_params()
        app_params = {}
        task_id = self.rtm.create_task(golem_params, app_params)
        return task_id

    def _mock_client_create(self):
        mock_client = Mock(spec=RequestorAppClient)
        create_f = asyncio.Future()
        create_f.set_result(mock_client)
        self._patch_async(
            'golem.task.requestedtaskmanager.RequestorAppClient.create',
            return_value=create_f)

        shutdown_f = asyncio.Future()
        shutdown_f.set_result(None)
        mock_client.shutdown = Mock(return_value=shutdown_f)
        return mock_client

    @staticmethod
    def _add_create_task_to_client_mock(mock_client):
        f = asyncio.Future()
        f.set_result(None)
        mock_client.create_task = Mock(return_value=f)

    @staticmethod
    def _add_has_pending_subtasks_to_client_mock(mock_client, result=True):
        f = asyncio.Future()
        f.set_result(result)
        mock_client.has_pending_subtasks = Mock(return_value=f)

    def _add_next_subtask_to_client_mock(self, mock_client):
        result = Mock(spec=Subtask)
        result.params = '{}'
        result.resources = '[]'
        f = asyncio.Future()
        f.set_result(result)
        mock_client.next_subtask = Mock(return_value=f)
        # next_subtask always also needs pending subtasks
        self._add_has_pending_subtasks_to_client_mock(mock_client)
        return result

    @staticmethod
    def _add_verify_to_client_mock(mock_client, result=True):
        f = asyncio.Future()
        f.set_result(result)
        mock_client.verify = Mock(return_value=f)

    def _patch_async(self, name, *args, **kwargs):
        patcher = monkeypatch(name, *args, **kwargs)
        self.addCleanup(patcher.stop)
        return patcher.start()
