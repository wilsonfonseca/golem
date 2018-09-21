from typing import Callable, Dict, Optional, Any

from golem_verificator.core_verifier import CoreVerifier


class RunFVerifier(CoreVerifier):
    # subtask_info is what sits in the task.subtasks_given["subtask_id"]
    # it is set in the query_extra_data
    def __init__(self, *_, verification_data: Optional[Dict[str, Any]] = None, **__) -> None:
        super().__init__()
        if verification_data:
            self.subtask_info = verification_data["subtask_info"]
        else:
            self.subtask_info = None

    # def start_verification(self, verification_data):
    #     self.results = verification_data["results"]
