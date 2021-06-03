import logging
import dill

from typing import Optional

import apache_beam
import apache_beam.io.filesystems
import apache_beam.options.value_provider
import apache_beam.options.pipeline_options
import apache_beam.utils.plugin

from votakvot import core
from votakvot.data import FancyDict, path_fs


logger = logging.getLogger(__name__)


class VotakvotOptions(apache_beam.options.pipeline_options.PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--votakvot_trial_path",
            type=str,
            default="",
            help="Path to directory with votakvot.yaml",
        )


def inject_trial_into_pipeline(
    pipeline: Optional[apache_beam.Pipeline] = None,
    *,
    options: Optional[apache_beam.options.pipeline_options.PipelineOptions] = None,
    context: Optional[core.TrackingContext] = None,
):
    if options and pipeline is None:
        pass
    elif pipeline and options is None:
        options = pipeline.options
    elif options is None and pipeline is None:
        options = _get_active_pipeline_options()
    else:
        raise ValueError("Parameters `options` and `pipeline` are mutually exclusives")

    context = context or core.current_context()
    assert isinstance(context, core.TrackingContext)
    inf_context = context.infused_context()

    fname = "infused_beam_context.pickle"
    fp = f"{context.path}/{fname}"

    if getattr(context, '_beam__tracked', False):
        logger.warning("Context is already attached to the pipeilne")
        return
    context._beam__tracked = True

    logger.info("Enable VotakvotBeamPlugin, snapshot path is %s", fp)
    options.view_as(VotakvotOptions).votakvot_trial_path = context.path
    so = options.view_as(apache_beam.options.pipeline_options.SetupOptions)
    so.beam_plugins = so.beam_plugins or []
    so.beam_plugins.append('votakvot.extras.beam_plugin.VotakvotBeamPlugin')

    logger.debug("Put pipeline options to context.meta")
    opts_as_dict = options.get_all_options()
    context.meta.setdefault('beam', FancyDict())
    context.meta['beam']['pipeline_options'] = opts_as_dict
    context.flush()

    with path_fs(fp).open(fp, 'wb') as f:
        dill.dump(inf_context, f)


def _get_active_pipeline_options():
    return apache_beam.io.filesystems.FileSystems._pipeline_options
