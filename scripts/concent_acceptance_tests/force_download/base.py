
from golem_messages import factories as msg_factories

from ..base import ConcentBaseTest


class ForceDownloadBaseTest(ConcentBaseTest):
    def get_fgtr(self, **kwargs):
        return msg_factories.concents.ForceGetTaskResultFactory(
            **self.gen_rtc_kwargs('report_computed_task__'),
            **{'report_computed_task__task_to_compute': self.gen_ttc()},
            **kwargs,
        )
