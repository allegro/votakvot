from __future__ import annotations

import abc
import datetime
import time

from typing import Any, Dict, Optional, Union

import votakvot


class resumable_fn(abc.ABC):

    snapshot_each: Optional[int] = None
    snapshot_period: Union[datetime.timedelta, float, None] = None

    def __init__(self, *args, **kwargs):
        self.index = 0
        self._args = args
        self._kwargs = kwargs
        self._state = 0
        self._result = None
        self._lsat = time.time()

    def __iter__(self):
        return self

    def _prepare_snapshot_period(self):
        if isinstance(self.snapshot_period, datetime.timedelta):
            self.snapshot_period = self.snapshot_period.total_seconds()

    def _need_snapshot(self):
        return (
            (self.snapshot_each and not self.index % self.snapshot_each)
            or (self.snapshot_period and time.time() > self.snapshot_period + self._lsat)
        )

    @classmethod
    def call(cls, *args, **kwargs):
        return next(filter(None, cls(*args, **kwargs)))

    def __getstate__(self):
        return self.save_state()

    def __setstate__(self, state):
        self.load_state(state)

    def __next__(self):

        if self._state == 0:    # begin
            self.init(*self._args, **self._kwargs)
            self._prepare_snapshot_period()
            self._state = 1
            self.snapshot()

        elif self._state == 1:  # loop
            self.index += 1
            if self.is_done():
                self._result = self.result()
                self._state = 3
                self.cleanup()
                self.snapshot()
            else:
                self.loop()
                if not self.is_done() and self._need_snapshot():
                    self.snapshot()

        elif self._state == 3:  # return
            self._state = 4
            return self._result

        else:
            raise StopIteration

    def snapshot(self):
        votakvot.current_tracker().snapshot()
        self._lsat = time.time()

    @abc.abstractmethod
    def init(self, *args, **kwargs) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def loop(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def is_done(self) -> bool:
        raise NotImplementedError

    def result(self) -> Any:
        return None

    def cleanup(self) -> None:
        pass

    def load_state(self, state: Dict):
        self.__dict__.update(state)

    def save_state(self) -> Dict:
        return self.__dict__
