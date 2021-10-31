import dill
import logging
import time
import threading

import votakvot

try:
    from dataflow_worker import batchworker
except ImportError:
    batchworker = None

from typing import Dict

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.utils.plugin import BeamPlugin

from votakvot import core
from votakvot.data import path_fs


logger = logging.getLogger(__name__)


class VotakvotBeamPlugin(BeamPlugin):
    """
    This class exists only to trigger to load this module
    spawn daemon thread which waits when PipelineOptions become
    available, then tries to load & install infused tracker
    """


_global_tracker_ctx = None


if batchworker:
    # Dataflow likes to kill worker processes with SIGKILL, which makes `atexit` useless
    # Here we are trying to hook into Dataflow bootstrap machinery
    # in order to flush contexts before process actually dies

    logger.info("hook into `dataflow_worker.batchworker.BatchWorker` ;)")

    # Flush metrics & info before worker finishes workitem
    def do_work_patched(self, work_item, deferred_exception_details=None):
        logger.debug("hooked do_work %s", work_item)
        res = self.do_work_original(
            work_item,
            deferred_exception_details=deferred_exception_details,
        )
        ctx = _global_tracker_ctx
        if ctx:
            ctx.__exit__(None, None, None)
        return res

    batchworker.BatchWorker.do_work_original = batchworker.BatchWorker.do_work
    batchworker.BatchWorker.do_work = do_work_patched

else:
    logger.info("don't install dataflow worker hooks")


def _current_pipeline_options() -> Dict:
    res = (
        None
        # _hacky_ way get current instance of `PipelineOptions`
        # when we are inside driver process or direct runner
        or FileSystems._pipeline_options
        # semi-official way to get pipeline options from worker runtime
        or RuntimeValueProvider.runtime_options
        # ups, no pipeline optinos (yet?)
        or {}
    )
    if isinstance(res, PipelineOptions):
        return res.get_all_options()
    else:
        return res


def _maybe_load_context(opts):
    logger.info("pipeline options are %s", opts)

    path = opts.get('votakvot_trial_path', "")
    if path:
        tracker_file = f"{path}/beam_infused_tracker.pickle"
        logger.info("load infused tracker from %s", tracker_file)
        with path_fs(tracker_file).open(tracker_file, mode='rb') as f:
            tracker = dill.load(f)

        global _global_tracker_ctx
        _global_tracker_ctx = votakvot.using_tracker(tracker, globally=True)
        _global_tracker_ctx.__enter__()

    else:
        logger.info("no infused tracker is provided")


def _wait_and_load_pipeline_options_run():
    logger.debug("wait for the PipelineOptions")

    phi = (1 + 5 ** 0.5) / 2
    delay = 0.01
    started = time.time()

    while started + 120 > time.time():
        opts = _current_pipeline_options()
        if opts:
            _maybe_load_context(opts)
            return
        else:
            logger.debug("pipline options are still unavaliable, sleep...")
            time.sleep(delay)
            delay *= phi

    logger.info("no pipeline optinos are")


# Wait until pipeline options became available.
# This code was tested for DataflowRunner,
# but it is generic enough to work with other runners.
_load_infused_context_thread = threading.Thread(
    daemon=True,
    name="votakvot-beam-plugin-wait-pipeline-opts",
    target=_wait_and_load_pipeline_options_run,
)
_load_infused_context_thread.start()
