"""P0: global optimistic validation with no application retry."""
from .base import Approach, Validation


class P0GlobalOCC(Approach):
    name = "p0"

    def validate(self, base, current, operation, affected_ids):
        return Validation(self.global_unchanged(base, current), "global")

