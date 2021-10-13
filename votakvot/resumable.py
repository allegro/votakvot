import abc
import datetime
import time

from typing import Any, Optional, Union

import votakvot


class resumable_fn(abc.ABC):

    snapshot_each: Optional[int] = None
    snapshot_period: Union[datetime.timedelta, float, None] = None

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._state = 0
        self._result = None

        self._cnt = 0
        self._lsat = time.time()

    def __iter__(self):
        return self

    def _prepare_snapshot_period(self):
        if isinstance(self.snapshot_period, datetime.timedelta):
            self.snapshot_period = self.snapshot_period.total_seconds()

    def _need_snapshot(self):
        return (
            (self.snapshot_each and not self._cnt % self.snapshot_each)
            or (self.snapshot_period and time.time() > self.snapshot_period + self._lsat)
        )

    @classmethod
    def call(cls, *args, **kwargs):
        return next(filter(None, cls(*args, **kwargs)))

    def __next__(self):

        if self._state == 0:    # begin
            self.init(*self._args, **self._kwargs)
            self._prepare_snapshot_period()
            self._state = 1
            self.snapshot()

        elif self._state == 1:  # loop
            self._cnt += 1
            self._result = self.loop()
            if self._result is None:
                if self._need_snapshot():
                    self.snapshot()
            else:
                self._state = 3
                self.clean()
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
    def loop(self) -> Optional[Any]:
        raise NotImplementedError

    def clean(self) -> None:
        pass
