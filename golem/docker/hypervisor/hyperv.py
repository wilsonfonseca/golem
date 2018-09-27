import logging
from os import path
from pathlib import Path
import subprocess
from subprocess import CalledProcessError, TimeoutExpired
from typing import Optional, Union, Any, List, Dict, ClassVar

from os_win.exceptions import OSWinException
from os_win.utils import _wqlutils
from os_win.utils.compute.vmutils import VMUtils

from golem.core.common import get_golem_path
from golem.docker import smbshare
from golem.docker.client import local_client
from golem.docker.config import CONSTRAINT_KEYS, MIN_CONSTRAINTS
from golem.docker.hypervisor.docker_machine import DockerMachineHypervisor
from golem.docker.job import DockerJob
from golem.docker.task_thread import DockerDirMapping

logger = logging.getLogger(__name__)


class HyperVHypervisor(DockerMachineHypervisor):

    DRIVER_NAME: ClassVar[str] = 'hyperv'
    OPTIONS = dict(
        mem='--hyperv-memory',
        cpu='--hyperv-cpu-count',
        disk='--hyperv-disk-size',
        no_virt_mem='--hyperv-disable-dynamic-memory',
        boot2docker_url='--hyperv-boot2docker-url',
        virtual_switch='--hyperv-virtual-switch'
    )
    SUMMARY_KEYS = dict(
        memory_size='MemoryUsage',
        cpu_count='NumberOfProcessors'
    )
    BOOT2DOCKER_URL = "https://github.com/golemfactory/boot2docker/releases/" \
                      "download/v18.06.0-ce%2Bdvm-v0.35/boot2docker.iso"
    DOCKER_USER = "golem-docker"
    DOCKER_PASSWORD = "golem-docker"
    VIRTUAL_SWITCH = "Golem Switch"
    VOLUME_DRIVER = "cifs"

    GET_IP_SCRIPT_PATH = \
        path.join(get_golem_path(), 'scripts', 'get-ip-address.ps1')
    SCRIPT_TIMEOUT = 5  # seconds

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vm_utils = VMUtilsWithMem()

    # pylint: disable=arguments-differ
    def _parse_create_params(
            self,
            cpu: Optional[Union[str, int]] = None,
            mem: Optional[Union[str, int]] = None,
            **params: Any) -> List[str]:

        args = super()._parse_create_params(**params)
        args += [self.OPTIONS['boot2docker_url'], self.BOOT2DOCKER_URL,
                 self.OPTIONS['virtual_switch'], self.VIRTUAL_SWITCH,
                 self.OPTIONS['no_virt_mem']]

        if cpu is not None:
            args += [self.OPTIONS['cpu'], str(cpu)]
        if mem is not None:
            args += [self.OPTIONS['mem'], str(mem)]

        return args

    def constraints(self, name: Optional[str] = None) -> Dict:
        name = name or self._vm_name
        try:
            summary = self._vm_utils.get_vm_summary_info(name)
            logger.debug('raw hyperv summary: %r', summary)
            result = {k: summary[v] for k, v in self.SUMMARY_KEYS.items()}
            limit = self._vm_utils.get_vm_memory_limit(name)
            result['memory_size'] = limit
            return result
        except (OSWinException, KeyError):
            logger.exception(
                f'Hyper-V: reading configuration of VM "{name}" failed')
            return {}

    def constrain(self, name: Optional[str] = None, **params) -> None:
        name = name or self._vm_name
        mem_key = CONSTRAINT_KEYS['mem']
        mem = params.get(mem_key)
        cpu = params.get(CONSTRAINT_KEYS['cpu'])

        min_mem = MIN_CONSTRAINTS[mem_key]
        dyn_mem_ratio = mem / min_mem

        try:
            self._vm_utils.update_vm(
                vm_name=name,
                memory_mb=mem,
                memory_per_numa_node=0,
                vcpus_num=cpu,
                vcpus_per_numa_node=0,
                limit_cpu_features=False,
                dynamic_mem_ratio=dyn_mem_ratio
            )
        except OSWinException:
            logger.exception(f'Hyper-V: reconfiguration of VM "{name}" failed')

        logger.info('Hyper-V: reconfiguration of VM "%s" finished', name)

    def update_work_dir(self, work_dir: Path) -> None:
        super().update_work_dir(work_dir)
        # Ensure that working directory is shared via SMB
        smbshare.create_share(self.DOCKER_USER, work_dir)

    @classmethod
    def _get_ip_for_sharing(cls) -> str:
        """
        Get IP address of the host machine which could be used for sharing
        directories with Hyper-V VMs connected to Golem's virtual switch.
        """
        try:
            return subprocess\
                .run(
                    [
                        'powershell.exe',
                        '-ExecutionPolicy', 'RemoteSigned',
                        '-File', cls.GET_IP_SCRIPT_PATH,
                        '-Interface', cls.VIRTUAL_SWITCH,
                    ],
                    timeout=10,  # seconds
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )\
                .stdout\
                .decode('utf8')\
                .strip()
        except (CalledProcessError, TimeoutExpired) as exc:
            raise RuntimeError(exc.stderr.decode('utf8'))

    @staticmethod
    def uses_volumes() -> bool:
        return True

    def create_volumes(self, dir_mapping: DockerDirMapping) -> dict:
        my_ip = self._get_ip_for_sharing()
        work_share = self._create_volume(my_ip, dir_mapping.work)
        res_share = self._create_volume(my_ip, Path(dir_mapping.resources))
        out_share = self._create_volume(my_ip, dir_mapping.output)

        return {
            work_share: {
                "bind": DockerJob.WORK_DIR,
                "mode": "rw"
            },
            res_share: {
                "bind": DockerJob.RESOURCES_DIR,
                "mode": "rw"
            },
            out_share: {
                "bind": DockerJob.OUTPUT_DIR,
                "mode": "rw"
            }
        }

    def _create_volume(self, my_ip: str, shared_dir: Path) -> str:
        assert self._work_dir is not None
        try:
            relpath = shared_dir.relative_to(self._work_dir)
        except ValueError:
            raise ValueError(
                f'Cannot create docker volume: "{shared_dir}" is not a '
                f'subdirectory of docker work dir ("{self._work_dir}")')

        share_name = smbshare.get_share_name(self._work_dir)
        volume_name = f'{my_ip}/{share_name}/{relpath.as_posix()}'

        # Client must be created here, do it in __init__() will not work since
        # environment variables are not set yet when __init__() is called
        client = local_client()
        client.create_volume(
            name=volume_name,
            driver=self.VOLUME_DRIVER,
            driver_opts={
                'username': self.DOCKER_USER,
                'password': self.DOCKER_PASSWORD
            }
        )

        return volume_name

class VMUtilsWithMem(VMUtils):
    def get_vm_memory_limit(self, vm_name):
        vmsetting = self._lookup_vm_check(vm_name)
        si = _wqlutils.get_element_associated_class(
            self._conn, self._MEMORY_SETTING_DATA_CLASS,
            element_instance_id=vmsetting.InstanceID)[0]
            
        logger.debug('VM MemorySettingsData: %r', si)
        return int(si.Limit)
