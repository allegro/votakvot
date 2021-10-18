from __future__ import annotations
from typing import Iterable

import votakvot.core


class Hook:

    def on_tracker_start(self, tracker: votakvot.core.Tracker):
        pass

    def on_tracker_flush(self, tracker: votakvot.core.Tracker):
        pass

    def on_tracker_finish(self, tracker: votakvot.core.Tracker):
        pass

    def on_tracker_infused(self, tracker: votakvot.core.InfusedTracker):
        pass


class HooksCollection(Hook):

    def __init__(self, hook):
        self.hook = list(hook or [])

    def _mk_run(method_name):
        def _run(self, *args):
            for h in self.hook:
                getattr(h, method_name)(*args)
        return _run

    for method_name in dir(Hook):
        if not method_name.startswith("_"):
            locals()[method_name] = _mk_run(method_name)


def coerce_to_hook(hook: Hook | Iterable[Hook] | None) -> Hook:
    if hook is None:
        return Hook()
    elif isinstance(hook, Hook):
        return hook
    else:
        return HooksCollection(list(hook))

