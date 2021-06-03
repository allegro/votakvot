import logging

import pandas as pd

from .core import current_context, Trial
from .data import path_fs, maybe_plainify


logger = logging.getLogger(__name__)


def load_trials(path=None, safe=True):
    path = path or getattr(current_context(), 'path', None) or "."
    fs = path_fs(path)
    clue_files = fs.glob(f"{path}/**/votakvot.yaml")

    trials = {}
    for f in clue_files:
        try:
            v = Trial(f.rsplit("/", 1)[0], _fs=fs)
            trials[v.tid] = v
        except Exception:
            if not safe:
                raise
            logger.exception("uanble to load %s", f)

    return trials


def _load_report(path, rowfn, safe):
    path = path or getattr(current_context(), 'path', None) or "."
    trials = load_trials(path)

    def yield_rows():
        for tid, v in trials.items():
            try:
                row = {'trial': v, **rowfn(v)}
            except Exception:
                if not safe:
                    raise
                logger.exception("uanble to load %s", tid)
            else:
                yield row

    df = pd.DataFrame(yield_rows())
    return df


def load_report(path=None, full=False, safe=True):

    if full:
        rowfn = lambda v: v.data_plain
    else:
        rowfn = lambda v: {
            **maybe_plainify(v.params, 'params'),
            **maybe_plainify(v.info, 'info'),
            **maybe_plainify(v.result, 'result'),
        }

    return _load_report(path, rowfn, safe)
