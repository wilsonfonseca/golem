import logging
import time
from collections import defaultdict
from threading import Lock
from typing import NamedTuple, Optional

from pydispatch import dispatcher

from golem.core.statskeeper import StatsKeeper
from golem.task.taskstate import Operation, TaskOp, SubtaskOp, \
    SubtaskStatus, TaskStatus, TaskState

__all__ = ['RequestorTaskStatsManager']

logger = logging.getLogger(__name__)

TaskMsg = NamedTuple("TaskMsg", [("ts", float), ("op", Operation)])


class SubtaskInfo:
    def __init__(self):
        self.latest_status = SubtaskStatus.starting
        self.messages = []


class TaskInfo:
    """Stores information about events related to the task.

    Stores information about events that were related to a single task and
    processes those information to get statistical information. It is probably
    only useful for :py:class:`RequestorTaskStats` objects which fill instances
    of this class with information.
    """

    def __init__(self):
        self.latest_status = TaskStatus.notStarted  # type: TaskStatus
        self._want_to_compute_count = 0
        self.messages = []  # type: List[TaskMsg]
        self.subtasks = defaultdict(
            SubtaskInfo)  # type: DefaultDict[str, SubtaskInfo]

    def got_want_to_compute(self):
        """Makes note of a received work offer"""
        self._want_to_compute_count += 1
        logger.info('Received work offers: %r', self._want_to_compute_count)

    def got_task_message(self, msg: TaskMsg, latest_status: TaskStatus):
        """Stores information from task level message"""
        self.messages.append(msg)
        self.latest_status = latest_status

    def got_subtask_message(self, subtask_id: str, msg: TaskMsg,
                            latest_status: SubtaskStatus):
        """Stores information from subtask level message"""
        self.subtasks[subtask_id].latest_status = latest_status
        self.subtasks[subtask_id].messages.append(msg)

    def subtask_count(self) -> int:
        """Number of subtasks of this task"""
        return len(self.subtasks.keys())

    def collected_results_count(self) -> int:
        """Returns number of successfully received results

        This is just a sum of verified and not accepted counts. That does not
        take "unexpected" results into account, that is results received
        which were not previously requested.
        """
        return (self.verified_results_count() +
                self.not_accepted_results_count())

    def verified_results_count(self) -> int:
        """Number of verified results of the subtasks for self task

        This is equal to the number of subtasks with the latest state
        ``SubtaskStatus.finished``.
        """
        cnt = 0
        for st in self.subtasks.values():
            if st.latest_status == SubtaskStatus.finished:
                cnt += 1
        return cnt

    def _subtasks_count_specific_ops(self, op: Operation):
        cnt = 0
        for st in self.subtasks.values():
            for msg in st.messages:
                if msg.op == op:
                    cnt += 1
        return cnt

    def not_accepted_results_count(self) -> int:
        """Number of times a subtask failed verification"""
        return self._subtasks_count_specific_ops(
            SubtaskOp.NOT_ACCEPTED)

    def timeout_count(self) -> int:
        """Number of times a subtask has not beed finished in time"""
        return self._subtasks_count_specific_ops(
            SubtaskOp.TIMEOUT)

    def failed_count(self) -> int:
        """Number of subtasks that failed on computing side"""
        return self._subtasks_count_specific_ops(
            SubtaskOp.FAILED)

    def not_downloaded_count(self) -> int:
        """Returns # of subtasks that were reported as computed but their
        results were never downloaded.

        Subtask is considered `not downloaded`, when there was
        RESULT_DOWNLOADING msg, not followed by FINISHED nor NOT_ACCEPTED.
        Other msgs are ignored.

        Note that if executed for a task that is still in progress this will
        also include subtasks that are actively sending results at the moment
        of a call.
        """
        cnt = 0
        for st in self.subtasks.values():
            download_in_progress = False
            for msg in st.messages:
                if msg.op == SubtaskOp.RESULT_DOWNLOADING:
                    download_in_progress = True
                elif msg.op in [SubtaskOp.FINISHED,
                                SubtaskOp.NOT_ACCEPTED]:
                    download_in_progress = False
            if download_in_progress:
                cnt += 1
        return cnt

    def total_time(self) -> float:
        """Returns total time in seconds spent on the task

        It is calculated as a wall time between ``TASK_CREATED`` or
        ``TASK_RESTORED`` and one of ``TASK_FINISHED``, ``TASK_NOT_ACCEPTED``,
        ``TASK_ABORTED`` and ``TASK_TIMEOUT`` messages. If the
        task is in progress then current time is taken instead of the
        latter. Note that the time spent paused is also included in
        the total time.
        """
        start_time = 0.0
        finish_time = 0.0

        if not self.is_completed():
            finish_time = time.time()

        for msg in reversed(self.messages):
            if (msg.op in [TaskOp.CREATED, TaskOp.RESTORED]
                    and not start_time):
                start_time = msg.ts
            elif msg.op.is_completed() and not finish_time:
                finish_time = msg.ts

        assert finish_time >= start_time
        return finish_time - start_time

    def had_failures_or_timeouts(self) -> bool:
        """Were there any failures or timeouts during computation

        Both failure to calculate (SUBTASK_FAILED) and failure to verify
        (SUBTASK_NOT_ACCEPTED) are considered failures in this method.
        """
        for msg in self.messages:
            if msg.op in [TaskOp.NOT_ACCEPTED,
                          TaskOp.TIMEOUT]:
                return True
        for st in self.subtasks.values():
            for msg in st.messages:
                if msg.op in [SubtaskOp.FAILED,
                              SubtaskOp.NOT_ACCEPTED,
                              SubtaskOp.TIMEOUT]:
                    return True
        return False

    def is_completed(self) -> bool:
        """Has the task already been completed

        In other words, is its latest status in the list of finished.
        """
        return self.latest_status.is_completed()

    def has_task_failed(self) -> bool:
        """Has the task failed

        If true it means that the whole task failed which is different
        from subtasks failing, which are reported via
        ``had_failures_or_timeouts()``
        """
        return self.latest_status in [TaskStatus.aborted, TaskStatus.timeout]

    def want_to_compute_count(self) -> int:
        """How many computation offers were received for this task"""
        return self._want_to_compute_count

    def in_progress_subtasks_count(self) -> int:
        """How many subtasks of this task are still being computed

        No tasks are considered to be in progress if the whole task has
        been completed, even if their individual statuses show
        otherwise.
        """
        if self.is_completed():
            return 0

        cnt = 0
        for st in self.subtasks.values():
            if st.latest_status in [SubtaskStatus.finished,
                                    SubtaskStatus.failure]:
                continue
            in_progress = False
            for msg in st.messages:
                if msg.op == SubtaskOp.ASSIGNED:
                    in_progress = True
                elif msg.op in [SubtaskOp.TIMEOUT,
                                SubtaskOp.FINISHED,
                                SubtaskOp.FAILED,
                                SubtaskOp.NOT_ACCEPTED]:
                    in_progress = False
            if in_progress:
                cnt += 1
        return cnt


TaskStats = NamedTuple("TaskStats", [("finished", bool),
                                     ("total_time", float),
                                     ("task_failed", bool),
                                     ("had_failures", bool),
                                     ("work_offers_cnt", int),
                                     ("requested_subtasks_cnt", int),
                                     ("collected_results_cnt", int),
                                     ("verified_results_cnt", int),
                                     ("timed_out_subtasks_cnt", int),
                                     ("not_downloaded_subtasks_cnt", int),
                                     ("failed_subtasks_cnt", int)])
TaskStats.__doc__ = """Information about a single task requested by this node

Names of fields are mostly self-explanatory.
``not_downloaded_subtasks_cnt`` is the number of tasks that were
announced as done by the computing node but were not received.
"""

EMPTY_TASK_STATS = TaskStats(False, 0.0, False, False, 0, 0, 0, 0, 0, 0, 0)

CurrentStats = NamedTuple("CurrentStats", [
    ("tasks_cnt", int),
    ("finished_task_cnt", int),
    ("requested_subtasks_cnt", int),
    ("collected_results_cnt", int),
    ("verified_results_cnt", int),
    ("timed_out_subtasks_cnt", int),
    ("not_downloadable_subtasks_cnt", int),
    ("failed_subtasks_cnt", int),
    ("work_offers_cnt", int)])
CurrentStats.__doc__ = """Statistics about a set of tasks

Intended to be used as a summary of information from a set of
`TaskStats`, this is periodically sent to the monitor.
"""

EMPTY_CURRENT_STATS = CurrentStats(0, 0, 0, 0, 0, 0, 0, 0, 0)


def update_current_stats_with_task(
        current: CurrentStats,
        old: Optional[TaskStats],
        new: TaskStats) -> CurrentStats:
    """Returns new :py:class:`CurrentStats` instance with changes
    between ``old`` and ``new`` incorporated into ``current``

    The ``not_downloadable_subtasks_cnt`` is only updated for tasks
    that are finished. Since it includes tasks that are downloaded at
    a time of a call, it would be misleading to update it earlier.

    Note that ``current`` is a tuple and can't be updated in place so
    a brand new one is returned.
    """
    is_new_task = old is None
    if is_new_task:
        old = EMPTY_TASK_STATS

    return CurrentStats(
        tasks_cnt=current.tasks_cnt + (1 if is_new_task else 0),
        finished_task_cnt=(current.finished_task_cnt
                           - (1 if old.finished else 0)
                           + (1 if new.finished else 0)),
        requested_subtasks_cnt=(current.requested_subtasks_cnt
                                - old.requested_subtasks_cnt
                                + new.requested_subtasks_cnt),
        collected_results_cnt=(current.collected_results_cnt
                               - old.collected_results_cnt
                               + new.collected_results_cnt),
        verified_results_cnt=(current.verified_results_cnt
                              - old.verified_results_cnt
                              + new.verified_results_cnt),
        timed_out_subtasks_cnt=(current.timed_out_subtasks_cnt
                                - old.timed_out_subtasks_cnt
                                + new.timed_out_subtasks_cnt),
        not_downloadable_subtasks_cnt=(
            current.not_downloadable_subtasks_cnt
            - (old.not_downloaded_subtasks_cnt if old.finished else 0)
            + (new.not_downloaded_subtasks_cnt if new.finished else 0)),
        failed_subtasks_cnt=(current.failed_subtasks_cnt
                             - old.failed_subtasks_cnt
                             + new.failed_subtasks_cnt),
        work_offers_cnt=(current.work_offers_cnt
                         - old.work_offers_cnt
                         + new.work_offers_cnt)
    )


FinishedTasksSummary = NamedTuple("FinishedTaskSummary", [
    ("tasks_cnt", int),
    ("total_time", float)])

EMPTY_FINISHED_SUMMARY = FinishedTasksSummary(0, 0.0)

FinishedTasksStats = NamedTuple("FinishedTasksStats", [
    ("finished_ok", FinishedTasksSummary),
    ("finished_with_failures", FinishedTasksSummary),
    ("failed", FinishedTasksSummary)])
FinishedTasksSummary.__doc__ = """Statistics about finished tasks

Divided into groups depending on the level of success: `finished_ok`
are tasks that were verified ok and had no problems along the way,
`finished_with_failures` are tasks that are verified ok in the end but
there were problems like timeouts or subtasks with errors, and `failed`
are tasks that did not succeed.
"""

EMPTY_FINISHED_STATS = FinishedTasksStats(
    EMPTY_FINISHED_SUMMARY,
    EMPTY_FINISHED_SUMMARY,
    EMPTY_FINISHED_SUMMARY)


def update_finished_stats_with_task(
        finished: FinishedTasksStats,
        old: Optional[TaskStats],
        new: TaskStats) -> FinishedTasksStats:
    mid = finished
    if old and old.finished:
        if old.task_failed:
            mid = finished._replace(
                failed=FinishedTasksSummary(
                    tasks_cnt=finished.failed.tasks_cnt - 1,
                    total_time=finished.failed.total_time - old.total_time))
        elif old.had_failures:
            mid = finished._replace(
                finished_with_failures=FinishedTasksSummary(
                    tasks_cnt=finished.finished_with_failures.tasks_cnt - 1,
                    total_time=(finished.finished_with_failures.total_time
                                - old.total_time)))
        else:
            mid = finished._replace(
                finished_ok=FinishedTasksSummary(
                    tasks_cnt=finished.finished_ok.tasks_cnt - 1,
                    total_time=(finished.finished_ok.total_time
                                - old.total_time)))
    ret = mid
    if new.finished:
        if new.task_failed:
            ret = mid._replace(
                failed=FinishedTasksSummary(
                    tasks_cnt=mid.failed.tasks_cnt + 1,
                    total_time=mid.failed.total_time + new.total_time))
        elif new.had_failures:
            ret = mid._replace(
                finished_with_failures=FinishedTasksSummary(
                    tasks_cnt=mid.finished_with_failures.tasks_cnt + 1,
                    total_time=(mid.finished_with_failures.total_time
                                + new.total_time)))
        else:
            ret = mid._replace(
                finished_ok=FinishedTasksSummary(
                    tasks_cnt=mid.finished_ok.tasks_cnt + 1,
                    total_time=mid.finished_ok.total_time + new.total_time))
    return ret


class RequestorTaskStats:
    """Collects statistics about our tasks.

    :py:class:`RequestorTaskStats` collects information about tasks requested
    by the user via ``on_message`` method and has two methods,
    :py:meth:`get_current_stats` and :py:meth:`get_finished_stats`, that are
    used for extracting information from it.
    """

    def __init__(self):
        self.tasks = defaultdict(
            TaskInfo)  # type: DefaultDict[str, TaskInfo]
        self.stats = EMPTY_CURRENT_STATS
        self.finished_stats = EMPTY_FINISHED_STATS

    def on_message(self,
                   task_id: str,
                   task_state: TaskState,
                   subtask_id: str = None,
                   op: Operation = None) -> None:
        """Updates stats according to the received information."""

        old_task_stats = None
        if task_id in self.tasks:
            old_task_stats = self.get_task_stats(task_id)

        if not op or op.unnoteworthy():
            pass

        elif op == TaskOp.WORK_OFFER_RECEIVED:
            self.tasks[task_id].got_want_to_compute()

        elif op == TaskOp.RESTORED:
            if task_state.status.is_completed():
                logger.debug("Skipping completed task %r", task_id)
            else:
                the_time = time.time()
                for s_id in task_state.subtask_states.keys():
                    subtask_status = (task_state.subtask_states[s_id]
                                      .status)
                    self.tasks[task_id].got_subtask_message(
                        s_id,
                        TaskMsg(ts=the_time, op=SubtaskOp.RESTARTED),
                        subtask_status)
                    if subtask_status in [SubtaskStatus.starting,
                                          SubtaskStatus.downloading]:
                        self.tasks[task_id].got_subtask_message(
                            s_id,
                            TaskMsg(ts=the_time, op=SubtaskOp.ASSIGNED),
                            subtask_status)

                msg = TaskMsg(ts=the_time, op=TaskOp.RESTORED)
                self.tasks[task_id].got_task_message(msg, task_state.status)

        elif op.task_related():
            self.tasks[task_id].got_task_message(
                TaskMsg(ts=time.time(), op=op),
                task_state.status)

        elif op.subtask_related():
            assert subtask_id
            self.tasks[task_id].got_subtask_message(
                subtask_id,
                TaskMsg(ts=time.time(), op=op),
                task_state.subtask_states[subtask_id].status)

        else:
            # Unknown operation, log problem
            logger.debug("Unknown operation %r", op.name)

        if task_id in self.tasks:
            new_task_stats = self.get_task_stats(task_id)
            self.stats = update_current_stats_with_task(
                self.stats, old_task_stats, new_task_stats)
            self.finished_stats = update_finished_stats_with_task(
                self.finished_stats, old_task_stats, new_task_stats)

    def is_task_finished(self, task_id: str) -> bool:
        """Returns True for a known, completed task"""
        ti = self.tasks.get(task_id)
        return bool(ti and ti.is_completed())

    def get_task_stats(self, task_id: str) -> TaskStats:
        """Returns statistical information about a single task

        It is best to call it on a finished task, as all the values
        will then be final. It will work on the task in progress, but
        some fields like ``not_downloaded_subtasks_cnt`` can decrease.
        """
        ti = self.tasks[task_id]  # type: TaskInfo
        return TaskStats(
            finished=ti.is_completed(),
            task_failed=ti.has_task_failed(),
            total_time=ti.total_time(),
            had_failures=ti.had_failures_or_timeouts(),
            work_offers_cnt=ti.want_to_compute_count(),
            requested_subtasks_cnt=ti.subtask_count(),
            collected_results_cnt=ti.collected_results_count(),
            verified_results_cnt=ti.verified_results_count(),
            timed_out_subtasks_cnt=ti.timeout_count(),
            not_downloaded_subtasks_cnt=ti.not_downloaded_count(),
            failed_subtasks_cnt=ti.failed_count())

    def get_current_stats(self) -> CurrentStats:
        """Returns information about current state of requested tasks."""
        return self.stats

    def get_finished_stats(self) -> FinishedTasksStats:
        """Returns stats about tasks that had been finished."""
        return self.finished_stats


class AggregateTaskStats:

    def __init__(self, **kwargs):
        # Number of subtasks paid (batch transfers)
        self.requestor_payment_cnt: int = 0
        # Average batch payment delay
        self.requestor_payment_delay_avg: float = 0.0
        # Sum of batch payment delays
        self.requestor_payment_delay_sum: float = 0.0
        # Subtask timeout multiplied by count
        self.requestor_subtask_timeout_mag: int = 0
        # Subtask price multiplied by count
        self.requestor_subtask_price_mag: int = 0
        # Sum of time spent on computations that timed out
        self.requestor_velocity_timeout: int = 0
        # Sum of total computation time, including failures and timeouts
        self.requestor_velocity_comp_time: int = 0

        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


class RequestorAggregateStatsManager:

    def __init__(self):
        self.keeper = StatsKeeper(AggregateTaskStats, default_value='0')
        self._payment_lock = Lock()
        self._computed_lock = Lock()

        dispatcher.connect(self._on_computed,
                           signal='golem.subtask')
        dispatcher.connect(self._on_payment,
                           signal="golem.payment")

    def _on_computed(self, event: str = 'default', **kwargs) -> None:
        if event != 'finished':
            return

        subtask_count = int(kwargs['subtask_count'])
        subtask_timeout = int(kwargs['subtask_timeout'])
        subtask_price = int(kwargs['subtask_price'])
        subtask_computation_time = int(kwargs['subtask_computation_time'])

        with self._computed_lock:
            self.keeper.increase_stat('requestor_subtask_timeout_mag',
                                      subtask_count * subtask_timeout)
            self.keeper.increase_stat('requestor_subtask_price_mag',
                                      subtask_count * subtask_price)

            if kwargs.get('timed_out', False):
                self.keeper.increase_stat('requestor_velocity_timeout',
                                          subtask_computation_time)
            self.keeper.increase_stat('requestor_velocity_comp_time',
                                      subtask_computation_time)

    def _on_payment(self, event: str = 'default', **kwargs) -> None:
        if event != 'confirmed':
            return

        delay = kwargs.get('delay')

        with self._payment_lock:
            _, payment_cnt = self.keeper.get_stats(
                'requestor_payment_cnt')
            _, payment_delay_sum = self.keeper.get_stats(
                'requestor_payment_delay_sum')

            new_cnt = payment_cnt + 1
            new_sum = payment_delay_sum + delay
            new_avg = new_sum / new_cnt

            self.keeper.set_stat('requestor_payment_cnt', new_cnt)
            self.keeper.set_stat('requestor_payment_delay_sum', new_sum)
            self.keeper.set_stat('requestor_payment_delay_avg', new_avg)


class RequestorTaskStatsManager:
    """Connects :py:class:`RequestorTaskStats` to pydispatcher.

    It learns about changes to the tasks via ``pydispatcher``
    signal ``golem.taskmanager`` with event ``task_status_updated``. This signal
    is normally emitted by :py:meth:`TaskManager.notice_task_updated` method.
    """

    def __init__(self):
        self.requestor_stats = RequestorTaskStats()
        self.aggregate_stats = RequestorAggregateStatsManager()
        dispatcher.connect(self.cb_message,
                           signal="golem.taskmanager",
                           sender=dispatcher.Any)

    def cb_message(self,  # pylint: disable=too-many-arguments
                   sender: str,  # pylint: disable=unused-argument
                   signal: str,  # pylint: disable=unused-argument
                   event: Optional[str],
                   task_id: str,
                   task_state: TaskState,
                   subtask_id: str = None,
                   op: Operation = None):
        """A callback for ``pydispatcher`` messages about tasks"""
        if event != 'task_status_updated' or not task_id or not op:
            return
        self.requestor_stats.on_message(task_id, task_state, subtask_id, op)

    def get_current_stats(self) -> CurrentStats:
        """See :py:meth:`RequestorTaskStats.get_current_stats`"""
        return self.requestor_stats.get_current_stats()

    def get_finished_stats(self) -> FinishedTasksStats:
        """See :py:meth:`RequestorTaskStats.get_finished_stats`"""
        return self.requestor_stats.get_finished_stats()

    def get_aggregate_stats(self) -> AggregateTaskStats:
        return self.aggregate_stats.keeper.global_stats
