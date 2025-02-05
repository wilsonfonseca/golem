"""Task related module with procedures exposed by RPC"""

import copy
import functools
import logging
import os.path
import re
import typing
from pathlib import Path

from ethereum.utils import denoms
from golem_messages import helpers as msg_helpers
from golem_messages.datastructures import masking
from twisted.internet import defer

from apps.core.task import coretask
from apps.rendering.task import framerenderingtask
from apps.rendering.task.renderingtask import RenderingTask
from golem.core import golem_async
from golem.core import common
from golem.core import simpleserializer
from golem.core.deferred import DeferredSeq
from golem.ethereum import exceptions as eth_exceptions
from golem.model import Actor
from golem.resource import resource
from golem.rpc import utils as rpc_utils
from golem.task import (
    taskbase,
    taskkeeper,
    taskstate,
    tasktester,
    requestedtaskmanager,
)

if typing.TYPE_CHECKING:
    from golem.client import Client  # noqa pylint: disable=unused-import

logger = logging.getLogger(__name__)
TASK_NAME_RE = re.compile(r"(\w|[\-\. ])+$")


def safe_run(errback):
    def wrapped(f):
        @functools.wraps(f)
        def curry(*args, **kwargs):
            try:
                result = f(*args, **kwargs)
            except Exception as e:  # pylint: disable=broad-except
                logger.debug('Full traceback', exc_info=e)
                return errback(e, *args, **kwargs)
            return result
        return curry
    return wrapped


class CreateTaskError(Exception):
    pass


def _validate_task_dict(client, task_dict) -> None:
    task_type = task_dict.get('type')
    known_task_types = list(client.apps_manager.task_types.keys())
    if task_type not in known_task_types:
        raise ValueError(
            f"Task type '{task_type}' unrecognized, "
            f"must be one of: {known_task_types}"
        )

    name = ""
    if 'name' in task_dict:
        task_dict['name'] = task_dict['name'].strip()
        name = task_dict['name']
    if len(name) < 4 or len(name) > 24:
        raise ValueError(
            "Length of task name cannot be less "
            "than 4 or more than 24 characters.")
    if not TASK_NAME_RE.match(name):
        raise ValueError(
            "Task name can only contain letters, numbers, "
            "spaces, underline, dash or dot.")
    if 'id' in task_dict:
        logger.warning("discarding the UUID from the preset")
        del task_dict['id']

    subtasks_count = task_dict.get('subtasks_count', 0)
    options = task_dict.get('options', {})
    optimize_total = bool(options.get('optimize_total', False))
    if subtasks_count and not optimize_total:
        computed_subtasks = framerenderingtask.calculate_subtasks_count(
            subtasks_count=subtasks_count,
            optimize_total=False,
            use_frames=options.get('frame_count', 1) > 1,
            frames=[None] * options.get('frame_count', 1),
        )
        if computed_subtasks != subtasks_count:
            raise ValueError(
                "Subtasks count {:d} is invalid."
                " Maybe use {:d} instead?".format(
                    subtasks_count,
                    computed_subtasks,
                )
            )

    if task_dict['concent_enabled']:
        if not client.concent_service.enabled:  # `enabled` implies `available`
            raise CreateTaskError(
                "Cannot create task with concent enabled when "
                "Concent Service is " +
                (
                    'switched off' if client.concent_service.available
                    else 'disabled'
                ),
            )
        if not client.apps_manager.get_app(
                task_dict['type']
        ).concent_supported:
            raise CreateTaskError(
                f"Concent is not supported for {task_dict['type']} tasks."
            )


def validate_client(client):
    if client.config_desc.in_shutdown:
        raise CreateTaskError(
            'Can not enqueue task: shutdown is in progress, '
            'toggle shutdown mode off to create new tasks.')
    if client.task_server is None:
        raise CreateTaskError("Golem is not ready")


def prepare_and_validate_task_dict(client, task_dict):
    task_type_id = task_dict.get('type', '').lower()
    task_dict['type'] = task_type_id
    # Set default value for concent_enabled
    task_dict.setdefault(
        'concent_enabled',
        client.concent_service.enabled and
        client.apps_manager.get_app(task_type_id).concent_supported
    )
    _validate_task_dict(client, task_dict)


@golem_async.deferred_run()
def _run_test_task(client, task_dict):

    def on_success(result, estimated_memory, time_spent, **kwargs):
        logger.info('Test task succes "%r"', task_dict)
        client.task_tester = None
        client.task_test_result = {
            "status": taskstate.TaskTestStatus.success,
            "result": result,
            "estimated_memory": estimated_memory,
            "time_spent": time_spent,
            "more": kwargs,
        }

    def on_error(*args, **kwargs):
        logger.warning('Test task error "%r": %r', task_dict, args)
        client.task_tester = None
        client.task_test_result = {
            "status": taskstate.TaskTestStatus.error,
            "error": args,
            "more": kwargs,
        }

    dictionary = simpleserializer.DictSerializer.load(task_dict)
    task = client.task_server.task_manager.create_task(
        dictionary=dictionary, test=True
    )

    client.task_test_result = {
        "status": taskstate.TaskTestStatus.started,
        "error": None,
    }
    client.task_tester = tasktester.TaskTester(
        task,
        client.task_server.get_task_computer_root(),
        on_success,
        on_error,
    )
    client.task_tester.run()


def _create_task(client: 'Client', task_dict: dict) -> taskbase.Task:
    validate_client(client)
    prepare_and_validate_task_dict(client, task_dict)
    return client.task_manager.create_task(task_dict)


def _prepare_task(
        client: 'Client',
        task: taskbase.Task,
        force: bool
) -> defer.Deferred:
    logger.debug('_prepare_task(). dict=%r', task.task_definition.to_dict())
    seq = DeferredSeq()
    seq.push(client.task_manager.initialize_task, task)
    seq.push(enqueue_new_task, client, task, force=force)
    return seq.execute()


def _restart_subtasks(
        client: 'Client',
        old_task_id: str,
        task_dict: dict,
        subtask_ids_to_copy: typing.Iterable[str],
        ignore_gas_price: bool = False,
):
    new_task = _create_task(client, task_dict)

    def _copy_results(*_):
        client.task_manager.copy_results(
            old_task_id=old_task_id,
            new_task_id=new_task.header.task_id,
            subtask_ids_to_copy=subtask_ids_to_copy
        )

    # Fire and forget the next steps after create_task
    deferred = _prepare_task(
        client=client,
        task=new_task,
        force=ignore_gas_price)
    deferred.addErrback(
        lambda failure: _restart_subtasks_error(
            e=failure.value,
            _self=None,
            task_id=new_task.header.task_id,
            subtask_ids=subtask_ids_to_copy
        )
    )
    deferred.addCallback(_copy_results)


@defer.inlineCallbacks
def _ensure_task_deposit(client, task, force):
    if not task.header.concent_enabled:
        return

    if not client.concent_service.available:
        return

    task_id = task.header.task_id
    task_state = client.task_manager.tasks_states[task_id]
    task_state.status = taskstate.TaskStatus.creatingDeposit
    min_amount, opt_amount = msg_helpers.requestor_deposit_amount(
        task.price,
    )
    logger.info(
        "Ensuring deposit. min=%.8f optimal=%.8f task_id=%r",
        min_amount / denoms.ether,
        opt_amount / denoms.ether,
        task_id,
    )
    # This is a bandaid solution for unlocking funds when task creation
    # fails. This case is most common but, the better way it to always
    # unlock them when the task fails regardless of the reason.
    try:
        client.transaction_system.validate_concent_deposit_possibility(
            required=min_amount,
            tasks_num=task.get_total_tasks(),
            force=force,
        )
        yield client.transaction_system.concent_deposit(
            required=min_amount,
            expected=opt_amount,
        )
    except eth_exceptions.EthereumError:
        client.funds_locker.remove_task(task_id)
        raise

    logger.info(
        "Deposit confirmed. task_id=%r",
        task_id,
    )


@defer.inlineCallbacks
def _create_task_package(client, task):
    files = resource.get_resources_for_task(
        resources=task.get_resources()
    )

    packager_result = yield client.resource_server.create_resource_package(
        files,
        task.header.task_id,
    )
    return packager_result


def _get_mask_for_task(client, task: coretask.CoreTask) -> masking.Mask:
    desired_num_workers = max(
        task.get_total_tasks() * client.config_desc.initial_mask_size_factor,
        client.config_desc.min_num_workers_for_mask,
    )

    if client.p2pservice is None:
        raise RuntimeError('P2PService not ready')
    if client.task_server is None:
        raise RuntimeError('TaskServer not ready')

    network_size = client.p2pservice.get_estimated_network_size()
    min_perf = client.task_server.get_min_performance_for_task(task)
    perf_rank = client.p2pservice.get_performance_percentile_rank(
        min_perf, task.header.environment)
    potential_num_workers = int(network_size * (1 - perf_rank))

    mask = masking.Mask.get_mask_for_task(
        desired_num_workers=desired_num_workers,
        potential_num_workers=potential_num_workers
    )
    logger.info(
        f'Task {task.header.task_id} '
        f'initial mask size: {mask.num_bits} '
        f'expected number of providers: {desired_num_workers} '
        f'potential number of providers: {potential_num_workers}'
    )

    return mask


@defer.inlineCallbacks
def add_resources(client, resources, res_id, timeout):
    files = copy.copy(list(resources))

    packager_result = yield client.resource_server.create_resource_package(
        files,
        res_id
    )
    package_path, package_sha1 = packager_result
    resource_size = os.path.getsize(package_path)
    client_options = client.task_server.get_share_options(timeout=timeout)
    resource_server_result = yield client.resource_server.add_resources(
        package_path,
        res_id,
        client_options=client_options,
    )

    logger.info("Resource package created. res_id=%r", res_id)
    return resource_server_result + (package_sha1, resource_size)


@defer.inlineCallbacks
def _setup_task_resources(client, task):
    task_id = task.header.task_id

    if client.config_desc.net_masking_enabled:
        task.header.mask = _get_mask_for_task(
            client=client,
            task=task,
        )
    else:
        task.header.mask = masking.Mask()

    estimated_fee = client.transaction_system.eth_for_batch_payment(
        task.get_total_tasks())
    client.task_manager.add_new_task(task, estimated_fee=estimated_fee)

    resource_server_result = yield add_resources(
        client,
        task.get_resources(),
        task_id,
        common.deadline_to_timeout(task.header.deadline)
    )

    return resource_server_result


@golem_async.deferred_run()
def _start_task(client, task, resource_server_result):
    resource_manager_result, package_path,\
        package_hash, package_size = resource_server_result

    task_state = client.task_manager.tasks_states[task.header.task_id]
    task_state.package_path = package_path
    task_state.package_hash = package_hash
    task_state.package_size = package_size
    task_state.resource_hash = resource_manager_result[0]
    logger.debug(
        "Setting task state - package_path: %s, package_hash: %s, "
        "package_size: %s, resource_hash: %s",
        task_state.package_path, task_state.package_hash,
        task_state.package_size, task_state.resource_hash
    )

    client.task_manager.start_task(task.header.task_id)


@defer.inlineCallbacks
def enqueue_new_task(client, task, force=False) \
        -> typing.Generator[defer.Deferred, typing.Any, taskbase.Task]:
    """Feed a fresh Task to all golem subsystems"""
    validate_client(client)
    task_id = task.header.task_id
    client.funds_locker.lock_funds(
        task_id,
        task.subtask_price,
        task.get_total_tasks(),
    )
    logger.debug('Enqueue new task. task_id=%r', task)

    resource_server_result = yield _setup_task_resources(
        client=client,
        task=task,
    )

    logger.debug("Task resources created. task_id=%r", task_id)

    try:
        yield _ensure_task_deposit(
            client=client,
            task=task,
            force=force,
        )

        yield _start_task(
            client=client,
            task=task,
            resource_server_result=resource_server_result,
        )

        logger.info("Task started. task_id=%r", task_id)
    except eth_exceptions.EthereumError as e:
        logger.error(
            "Can't enqueue_new_task. task_id=%(task_id)r, e=%(e_name)s: %(e)s",
            {
                'task_id': task_id,
                'e': e,
                'e_name': e.__class__.__name__,
            },
        )
        raise
    except Exception:  # pylint: disable=broad-except
        logger.exception("Can't enqueue_new_task. task_id=%r", task_id)
        raise
    return task


def _create_task_error(e, _self, task_dict, *args, **_kwargs) \
        -> typing.Tuple[None, typing.Union[str, typing.Dict]]:
    _self.client.task_manager.task_creation_failed(task_dict.get('id'), str(e))

    if hasattr(e, 'to_dict'):
        return None, rpc_utils.int_to_string(e.to_dict())

    return None, str(e)


def _restart_task_error(e, _self, task_id, *args, **_kwargs) \
        -> typing.Tuple[None, str]:
    logger.error("Cannot restart task %r: %s", task_id, e)

    if hasattr(e, 'to_dict'):
        return None, rpc_utils.int_to_string(e.to_dict())

    return None, str(e)


def _restart_subtasks_error(e, _self, task_id, subtask_ids, *_args, **_kwargs) \
        -> typing.Union[str, typing.Dict]:
    logger.error("Failed to restart subtasks. task_id: %r, subtask_ids: %r, %s",
                 task_id, subtask_ids, e)

    if hasattr(e, 'to_dict'):
        return rpc_utils.int_to_string(e.to_dict())

    return str(e)


def _test_task_error(e, self, task_dict, **_kwargs):
    logger.error("Test task error: %s", e)
    logger.debug("Test task details. task_dict=%s", task_dict)
    self.client.task_test_result = {
        "status": taskstate.TaskTestStatus.error,
        "error": str(e),
    }
    return False


class ClientProvider:
    """Provides task related remote procedures that require Client"""

    # Add only methods that are exposed via RPC
    def __init__(self, client):
        self.client = client

    @property
    def task_manager(self):
        return self.client.task_server.task_manager

    @property
    def requested_task_manager(
            self,
    ) -> requestedtaskmanager.RequestedTaskManager:
        return self.client.task_server.requested_task_manager

    @rpc_utils.expose('comp.task.create')
    @safe_run(_create_task_error)
    def create_task(self, task_dict, force=False) \
            -> typing.Tuple[typing.Optional[str],
                            typing.Optional[typing.Union[str, typing.Dict]]]:
        """
        :param force: if True will ignore warnings
        :return: (task_id, None) on success; (task_id or None, error_message)
                 on failure
        """
        logger.info('Creating task. task_dict=%r', task_dict)
        logger.debug('force=%r', force)

        task = _create_task(self.client, task_dict)
        task_id = task.header.task_id

        try:
            self._validate_enough_funds_to_pay_for_task(
                task.subtask_price,
                task.get_total_tasks(),
                task.header.concent_enabled,
                force
            )
        except Exception as exc:  # pylint: disable=broad-except
            self.client.task_manager.task_creation_failed(task_id, str(exc))
            raise

        # Fire and forget the next steps after create_task
        deferred = _prepare_task(client=self.client, task=task, force=force)
        deferred.addErrback(
            lambda failure: _create_task_error(
                e=failure.value,
                _self=self,
                task_dict=task_dict,
                force=force
            )
        )

        return task_id, None

    @rpc_utils.expose('comp.task_api.create')
    def create_task_api_task(self, task_params: dict, golem_params: dict):
        logger.info('Creating Task API task. golem_params=%r', golem_params)

        create_task_params = requestedtaskmanager.CreateTaskParams(
            app_id=golem_params['app_id'],
            name=golem_params['name'],
            environment=golem_params['environment'],
            output_directory=Path(golem_params['output_directory']),
            resources=list(map(Path, golem_params['resources'])),
            max_price_per_hour=int(golem_params['max_price_per_hour']),
            max_subtasks=int(golem_params['max_subtasks']),
            task_timeout=int(golem_params['task_timeout']),
            subtask_timeout=int(golem_params['subtask_timeout']),
            concent_enabled=False,  # Concent doesn't support Task API
        )

        self._validate_enough_funds_to_pay_for_task(
            create_task_params.max_price_per_hour,
            create_task_params.max_subtasks,
            create_task_params.concent_enabled,
            False,
        )

        task_id = self.requested_task_manager.create_task(
            create_task_params,
            task_params,
        )

        self.client.funds_locker.lock_funds(
            task_id,
            create_task_params.max_price_per_hour,
            create_task_params.max_subtasks,
        )

        @defer.inlineCallbacks
        def init_task():
            try:
                self.requested_task_manager.init_task(task_id)
            except Exception:
                self.client.funds_locker.remove_task(task_id)
                raise
            else:
                self.requested_task_manager.start_task(task_id)
            # Dummy yield to make this function work with inlineCallbacks.
            # To be removed when there are other yeilds in this function.
            yield defer.Deferred()

        # Do not yield, this is a fire and forget deferred as it may take long
        # time to complete and shouldn't block the RPC call.
        d = init_task()
        d.addErrback(lambda e: logger.info("Task creation error %r", e))  # noqa pylint: disable=no-member

        return task_id

    def _validate_enough_funds_to_pay_for_task(
            self,
            subtask_price: int,
            subtask_count: int,
            concent_enabled: bool,
            force: bool
    ):
        self._validate_lock_funds_possibility(subtask_price, subtask_count)

        concent_available = self.client.concent_service.available
        if concent_enabled and concent_available:
            min_amount, _ = msg_helpers.requestor_deposit_amount(subtask_price)
            self.client.transaction_system.validate_concent_deposit_possibility(
                required=min_amount,
                tasks_num=subtask_count,
                force=force,
            )

    def _validate_lock_funds_possibility(
            self,
            subtask_price: int,
            subtask_count: int) -> None:
        total_price_gnt: int = subtask_price * subtask_count
        transaction_system = self.client.transaction_system
        missing_funds: typing.List[eth_exceptions.MissingFunds] = []

        gnt_available = transaction_system.get_available_gnt()
        if total_price_gnt > gnt_available:
            missing_funds.append(eth_exceptions.MissingFunds(
                required=total_price_gnt,
                available=gnt_available,
                currency='GNT'
            ))

        eth = transaction_system.eth_for_batch_payment(subtask_count)
        eth_available = transaction_system.get_available_eth()
        if eth > eth_available:
            missing_funds.append(eth_exceptions.MissingFunds(
                required=eth,
                available=eth_available,
                currency='ETH'
            ))

        if missing_funds:
            raise eth_exceptions.NotEnoughFunds(missing_funds)

    @rpc_utils.expose('comp.task.restart')
    @safe_run(_restart_task_error)
    def restart_task(
            self,
            task_id: str,
            force: bool = False,
            disable_concent: bool = False
    ) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
        """
        :return: (new_task_id, None) on success; (None, error_message)
                 on failure
        """
        logger.info('Restarting task. task_id=%r', task_id)
        logger.debug('force=%r, disable_concent=%r', force, disable_concent)

        # Task state is changed to restarted and stays this way until it's
        # deleted from task manager.
        try:
            self.task_manager.assert_task_can_be_restarted(task_id)
        except self.task_manager.AlreadyRestartedError:
            return None, "Task already restarted: '{}'".format(task_id)

        # Create new task that is a copy of the definition of the old one.
        # It has a new deadline and a new task id.
        try:
            task = self.task_manager.tasks[task_id]
            self._validate_enough_funds_to_pay_for_task(
                task.subtask_price,
                task.get_total_tasks(),
                False if disable_concent else task.header.concent_enabled,
                force
            )

            task_dict = copy.deepcopy(
                self.task_manager.get_task_definition_dict(
                    self.task_manager.tasks[task_id],
                ),
            )
        except KeyError:
            return None, "Task not found: '{}'".format(task_id)

        del task_dict['id']
        if disable_concent:
            task_dict['concent_enabled'] = False

        new_task = _create_task(self.client, task_dict)
        # Fire and forget the next steps after create_task
        deferred = _prepare_task(client=self.client, task=new_task, force=force)
        deferred.addErrback(
            lambda failure: _restart_task_error(
                e=failure.value,
                _self=self,
                task_id=task_id,
            )
        )
        self.task_manager.put_task_in_restarted_state(task_id)
        return new_task.header.task_id, None

    @rpc_utils.expose('comp.task.subtasks.restart')
    @safe_run(_restart_subtasks_error)
    def restart_subtasks(
            self,
            task_id: str,
            subtask_ids: typing.List[str],
            ignore_gas_price: bool = False,
            disable_concent: bool = False
    ) -> typing.Optional[typing.Union[str, typing.Dict]]:
        """
        Restarts a set of subtasks from the given task. If the specified task is
        already finished, all failed subtasks will be restarted along with the
        set provided as a parameter. Finished subtasks will have their results
        copied over to the newly created task.
        :param task_id: the ID of the task which contains the given subtasks.
        :param subtask_ids: the set of subtask IDs which should be restarted.
        If this is empty and the task is finished, all of the task's subtasks
        marked as failed will be restarted.
        :param ignore_gas_price: if True, this will ignore long transaction time
        errors and proceed with the restart.
        :param disable_concent: setting this flag to True will result in forcing
        Concent to be disabled for the task. This only has effect when the task
        is already finished and needs to be restarted.
        :return: In case of any errors, returns the representation of the error
        (either a string or a dict). Otherwise, returns None.
        """
        task = self.task_manager.tasks.get(task_id)
        if not task:
            return f'Task not found: {task_id!r}'

        subtasks_to_restart = set(subtask_ids)

        for sub_id in subtasks_to_restart:
            if self.task_manager.subtask_to_task(
                    sub_id, Actor.Requestor) != task_id:
                return f'Subtask does not belong to the given task.' \
                    f'task_id: {task_id}, subtask_id: {sub_id}'

        logger.info('Restarting subtasks. task_id=%r', task_id)
        logger.debug('restart_subtasks. subtask_ids=%r, ignore_gas_price=%r,'
                     'disable_concent=%r', subtask_ids, ignore_gas_price,
                     disable_concent)

        task_state = self.client.task_manager.tasks_states[task_id]

        if task_state.status.is_active():
            self._validate_enough_funds_to_pay_for_task(
                task.subtask_price,
                len(subtask_ids),
                task.header.concent_enabled,
                ignore_gas_price
            )

            for subtask_id in subtask_ids:
                self.client.restart_subtask(subtask_id)
        else:
            return self._restart_finished_task_subtasks(
                task_id,
                subtask_ids,
                ignore_gas_price,
                disable_concent
            )

        return None

    @rpc_utils.expose('comp.task.subtasks.frame.restart')
    @safe_run(
        lambda e, _self, task_id, frame: logger.error(
            'Frame restart failed. e=%r, task_id=%r, frame=%r',
            e, task_id, frame
        )
    )
    def restart_frame_subtasks(
            self,
            task_id: str,
            frame: int
    ) -> typing.Optional[typing.Union[str, typing.Dict]]:
        logger.debug('restart_frame_subtasks. task_id=%r, frame=%r',
                     task_id, frame)

        frame_subtasks: typing.FrozenSet[str] =\
            self.task_manager.get_frame_subtasks(task_id, frame)

        if not frame_subtasks:
            logger.error('Frame restart failed, frame has no subtasks.'
                         'task_id=%r, frame=%r', task_id, frame)
            return None

        return self.restart_subtasks(task_id, list(frame_subtasks))

    @safe_run(_restart_subtasks_error)
    def _restart_finished_task_subtasks(
            self,
            task_id: str,
            subtask_ids: typing.Iterable[str],
            ignore_gas_price: bool = False,
            disable_concent: bool = False
    ) -> typing.Optional[typing.Union[str, typing.Dict]]:
        logger.debug('_restart_finished_task_subtasks. task_id=%r, '
                     'subtask_ids=%r, ignore_gas_price=%r', task_id,
                     subtask_ids, ignore_gas_price)

        try:
            old_task = self.task_manager.tasks[task_id]

            finished_subtask_ids = set(
                sub_id for sub_id, sub in old_task.subtasks_given.items()
                if sub['status'] == taskstate.SubtaskStatus.finished
            )
            subtask_ids_to_copy = finished_subtask_ids - set(subtask_ids)

            self._validate_enough_funds_to_pay_for_task(
                old_task.subtask_price,
                old_task.get_total_tasks() - len(subtask_ids_to_copy),
                False if disable_concent else old_task.header.concent_enabled,
                ignore_gas_price
            )

            self.task_manager.put_task_in_restarted_state(
                task_id,
                clear_tmp=False,
            )

            logger.debug('_restart_finished_task_subtasks. '
                         'subtask_ids_to_copy=%r', subtask_ids_to_copy)
        except self.task_manager.AlreadyRestartedError:
            err_msg = f'Task already restarted: {task_id!r}'
            logger.error(err_msg)
            return err_msg
        except KeyError:
            err_msg = f'Task not found: {task_id!r}'
            logger.error(err_msg)
            return err_msg

        task_dict = copy.deepcopy(
            self.task_manager.get_task_definition_dict(old_task),
        )
        del task_dict['id']
        if disable_concent:
            task_dict['concent_enabled'] = False

        logger.debug('_restart_finished_task_subtasks. task_dict=%s', task_dict)
        _restart_subtasks(
            client=self.client,
            subtask_ids_to_copy=subtask_ids_to_copy,
            old_task_id=task_id,
            task_dict=task_dict,
            ignore_gas_price=ignore_gas_price,
        )
        # Don't wait for deferred

        return None

    @rpc_utils.expose('comp.tasks.check')
    @safe_run(_test_task_error)
    def run_test_task(self, task_dict) -> bool:
        logger.info('Running test task "%r" ...', task_dict)
        if self.client.task_tester is not None:
            self.client.task_test_result = {
                "status": taskstate.TaskTestStatus.error,
                "error": "Another test is running",
            }
            return False

        prepare_and_validate_task_dict(self.client, task_dict)
        self.client.task_test_result = None
        _run_test_task(
            client=self.client,
            task_dict=task_dict,
        )
        # Don't wait for _deferred
        return True

    @rpc_utils.expose('comp.task.subtasks.estimated.cost')
    def get_estimated_subtasks_cost(
            self,
            task_id: str,
            subtask_ids: typing.List[str]
    ) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
        """
        Estimates the cost of restarting an array of subtasks from a given task.
        If the specified task is finished, all of the failed subtasks from that
        task will be added to the estimation.
        :param task_id: ID of the task containing the subtasks to be restarted.
        :param subtask_ids: a list of subtask IDs which should be restarted. If
        one of the subtasks does not belong to the given task, an error will be
        returned.
        :return: a result, error tuple. When the result is present the error
        should be None (and vice-versa).
        """
        task = self.task_manager.tasks.get(task_id)
        if not task:
            return None, f'Task not found: {task_id}'

        subtasks_to_restart = set(subtask_ids)

        for sub_id in subtasks_to_restart:
            if self.task_manager.subtask_to_task(
                    sub_id, Actor.Requestor) != task_id:
                return None, f'Subtask does not belong to the given task.' \
                    f'task_id: {task_id}, subtask_id: {sub_id}'

        if self.task_manager.task_finished(task_id):
            failed_subtask_ids = set(
                sub_id for sub_id, subtask in task.subtasks_given.items()
                if subtask['status'] == taskstate.SubtaskStatus.failure
            )
            subtasks_to_restart |= failed_subtask_ids

        result = self._get_cost_estimation(
            len(subtasks_to_restart),
            task.subtask_price
        )

        return result, None

    @rpc_utils.expose('comp.tasks.estimated.cost')
    def get_estimated_cost(
            self,
            _task_type: str,
            options: typing.Optional[dict] = None,
            task_id: typing.Optional[str] = None,
            partial: typing.Optional[bool] = False
    ) -> typing.Tuple[typing.Optional[dict], typing.Optional[str]]:
        """
        Estimates the cost of a task. Result includes amounts required for both
        calculating the task, as well as creating a Concent deposit for it.

        :param _task_type: type of the task for which the cost should be
        estimated.
        :param options: task options, when provided and task_id parameter is
        None the cost estimation will be based on fields from this dict
        (i.e. price, subtask_count and subtask_timeout). Used for tasks
        which have not been created yet.
        :param task_id: if provided, the cost estimation will be based on an
        existing task with the given ID.
        :param partial: used in conjunction with the task_id parameter. If
        True, the estimation will only include unfinished subtasks of the
        specified task (i.e. estimating the cost of a partial task restart).
        Otherwise, the full task cost will be returned.
        :return: a tuple with the result dict as its first element and an error
        string as the second. When the result is present the error should be
        None (and vice-versa).
        """
        subtask_count: int = 0
        subtask_price: int = 0

        if task_id:
            task: taskbase.Task = self.task_manager.tasks.get(task_id)
            if not task:
                return None, f'Task not found: {task_id}'

            subtask_count = task.get_tasks_left() if partial else \
                task.get_total_tasks()
            subtask_price = task.subtask_price
        else:
            if not options:
                return None, 'You must pass either a task ID or task options.'

            subtask_count = int(options['subtasks_count'])
            subtask_timeout: int = common.string_to_timeout(
                options['subtask_timeout'],
            )
            subtask_price = taskkeeper.compute_subtask_value(
                price=int(options['price']),
                computation_time=subtask_timeout
            )

        result = self._get_cost_estimation(subtask_count, subtask_price)

        logger.info('Estimated task cost. result=%r', result)
        return result, None

    def _get_cost_estimation(self, subtask_count: int, subtask_price: int):
        estimated_gnt: int = subtask_count * subtask_price
        estimated_eth: int = self.client \
            .transaction_system.eth_for_batch_payment(subtask_count)
        estimated_gnt_deposit: typing.Tuple[int, int] = \
            msg_helpers.requestor_deposit_amount(estimated_gnt)
        estimated_deposit_eth: int = self.client.transaction_system \
            .eth_for_deposit()

        return {
            'GNT': str(estimated_gnt),
            'ETH': str(estimated_eth),
            'deposit': {
                'GNT_required': str(estimated_gnt_deposit[0]),
                'GNT_suggested': str(estimated_gnt_deposit[1]),
                'ETH': str(estimated_deposit_eth),
            },
        }

    @rpc_utils.expose('comp.task.rendering.task_fragments')
    def get_fragments(self, task_id: str) -> \
        typing.Tuple[
                typing.Optional[typing.Dict[int, typing.List[typing.Dict]]],
                typing.Optional[str]]:
        """
        Returns the task fragments for a given rendering task. A single task
        fragment is a collection of subtasks referring to the same, common part
        of the whole task. Fragments are identified using incremental integer
        indices.
        :param task_id: Task ID of the rendering task for which fragments should
        be obtained.
        :return: A dictionary where keys are the fragment indices and values are
        lists of subtasks associated with a given fragment. Returns None
        (along with an error message) if the task is not known or it is not a
        rendering task.
        """
        task = self.task_manager.tasks.get(task_id)
        if task is None:
            return None, f"Task not found: '{task_id}'"
        if not isinstance(task, RenderingTask):
            return None, f"Incorrect task type: '{task.__class__.__name__}'"

        fragments: typing.Dict[int, typing.List[typing.Dict]] = {}

        for subtask_index in range(1, task.total_tasks + 1):
            fragments[subtask_index] = []

        for subtask in self.task_manager.get_subtasks_dict(task_id) or []:
            fragments[subtask['extra_data']['start_task']].append(subtask)

        return fragments, None
