from pathlib import Path
from typing import List
from unittest import mock
import asyncio
import click
import json
import shutil
import tempfile
import time

from golem import database, model
from golem.core.common import install_reactor
from golem.task import envmanager, requestedtaskmanager, taskcomputer
from golem.task.task_api import docker
from golem.envs.auto_setup import auto_setup
from golem.envs.docker import cpu, whitelist

from twisted.internet.task import react
from twisted.internet.defer import ensureDeferred, Deferred

import logging
logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)


def as_deferred(f):
    return Deferred.fromFuture(asyncio.ensure_future(f))


async def test_task(
        work_dir: Path,
        environment: str,
        env_prerequisites_json: str,
        task_params_path: Path,
        resources: List[Path],
        max_subtasks: int,
) -> None:
    whitelist.Whitelist.add('blenderapp')  # TODO
    task_computer_work_dir = work_dir / 'computer'
    task_computer_work_dir.mkdir()
    env_manager = envmanager.EnvironmentManager()
    docker_cpu_config = cpu.DockerCPUConfig(work_dirs=[task_computer_work_dir])
    docker_cpu_env = auto_setup(cpu.DockerCPUEnvironment(docker_cpu_config))
    env_manager.register_env(
        docker_cpu_env,
        docker.DockerTaskApiPayloadBuilder,
    )
    env_manager.set_enabled(environment, True)

    env_prerequisites = json.loads(env_prerequisites_json)

    rtm_work_dir = work_dir / 'rtm'
    rtm_work_dir.mkdir()
    rtm = requestedtaskmanager.RequestedTaskManager(
        env_manager,
        b'',
        rtm_work_dir,
    )
    task_computer = taskcomputer.NewTaskComputer(
        env_manager,
        task_computer_work_dir,
    )
    output_dir = work_dir / 'output'
    output_dir.mkdir()
    golem_params = requestedtaskmanager.CreateTaskParams(
        app_id='',  # TODO
        name='testtask',
        environment=environment,
        task_timeout=3600,
        subtask_timeout=3600,
        output_directory=output_dir,
        resources=resources,
        max_subtasks=max_subtasks,
        max_price_per_hour=1,
        concent_enabled=False,
    )
    with open(task_params_path, 'r') as f:
        task_params = json.load(f)
    task_id = rtm.create_task(golem_params, task_params)
    for resource in resources:
        shutil.copy2(resource, rtm.get_resources_dir(task_id))
    print('Task created', task_id)
    await as_deferred(rtm.init_task(task_id))
    rtm.start_task(task_id)
    print('Task started')

    assert await as_deferred(rtm.has_pending_subtasks(task_id))
    computing_node = \
        requestedtaskmanager.ComputingNodeDefinition(node_id='id', name='test')
    while await as_deferred(rtm.has_pending_subtasks(task_id)):
        print('Getting next subtask')
        subtask_def = await as_deferred(rtm.get_next_subtask(
            task_id,
            computing_node,
        ))
        print('subtask', subtask_def)
        task_header = mock.Mock(
            task_id=task_id,
            environment=environment,
            environment_prerequisites=env_prerequisites,
            subtask_timeout=3600,
            deadline=time.time() + 3600,
        )
        ctd = {
            'subtask_id': subtask_def.subtask_id,
            'extra_data': subtask_def.params,
            'performance': 0,
            'deadline': time.time() + 3600,
        }
        task_computer.task_given(task_header, ctd)
        for resource in subtask_def.resources:
            shutil.copy2(
                rtm.get_task_network_resources_dir(task_id) / resource,
                task_computer.get_task_resources_dir(),
            )
        (task_computer.get_task_resources_dir().parent /
            subtask_def.subtask_id).mkdir()
        result_path = await task_computer.compute()
        shutil.copy2(
            result_path,
            rtm.get_subtasks_outputs_dir(task_id),
        )
        print('Starting verification')
        verdict = await as_deferred(rtm.verify(task_id, subtask_def.subtask_id))
        assert verdict
    print('Task completed')


@click.group()
def test():
    pass


async def _task(
        environment,
        env_prerequisites,
        task_params_path,
        resource,
        max_subtasks,
) -> None:
    work_dir = Path(tempfile.mkdtemp())
    print(work_dir)
    db = database.Database(
        model.db,
        fields=model.DB_FIELDS,
        models=model.DB_MODELS,
        db_dir=str(work_dir / 'database'),
    )
    try:
        await test_task(
            work_dir,
            environment,
            env_prerequisites,
            task_params_path,
            list(resource),
            max_subtasks,
        )
    finally:
        db.close()
        shutil.rmtree(work_dir)


@test.command()
@click.argument('environment', type=click.STRING)
@click.argument('env_prerequisites', type=click.STRING)
@click.argument('task_params_path', type=click.Path(exists=True))
@click.option('--resource', type=click.Path(exists=True), multiple=True)
@click.option('--max-subtasks', type=click.INT, default=2)
def task(
        environment,
        env_prerequisites,
        task_params_path,
        resource,
        max_subtasks,
):
    install_reactor()
    return react(
        lambda _reactor: ensureDeferred(
            _task(
                environment,
                env_prerequisites,
                task_params_path,
                resource,
                max_subtasks,
            )
        )
    )


if __name__ == '__main__':
    test()
