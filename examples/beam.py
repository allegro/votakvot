# run this script as:
#
# python examples/beam.py \
#   --work_items=20 \
#   --tries_per_wi=10000 \
#   --job-name=votakvot-ready-beam-job \
#   --runner=DataflowRunner \
#   --project=YOU_PROJECT_ID_HERE \
#   --region=YOU REGION HERE \
#   --temp_location=gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY \
#


import logging
import random
import json
import uuid
import argparse
import tempfile

from pathlib import Path
from typing import Tuple, Iterable, Any

import apache_beam as beam
import apache_beam.io as beam_io

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
)

import prometheus_client as pc

import votakvot
import votakvot.extras.beam
import votakvot.extras.gcm
import votakvot.extras.prometheus


logging.basicConfig(level=logging.DEBUG)


trials_counter = pc.Counter("trials_number", "Number of trials")
wi_time = pc.Histogram("wi_time", "Duration of singe WI")

metrics = [
    trials_counter,
    wi_time,
]


def run_trials(
    runs: int,
) -> Tuple[int, int, int]:

    inside_runs = 0
    trials_counter.inc(runs)

    with wi_time.time():
        for _ in range(runs):
            x = random.uniform(0, 1)
            y = random.uniform(0, 1)
            inside_runs += x * x + y * y <= 1
        return runs, inside_runs, 0


def serialize_result(res):
    votakvot.inform(
        result=res,
    )
    return json.dumps(res)


def combine_results(
    results: Iterable[Tuple[int, int, Any]],
) -> Tuple[int, int, float]:
    total, inside = sum(r[0] for r in results), sum(r[1] for r in results)
    votakvot.meter(total=total, inside=inside)
    return total, inside, 4 * float(inside) / total


@votakvot.track()
def run_pipeline(
    output,
    work_items,
    tries_per_wi,
    _pipeline_options,
):
    with beam.Pipeline(options=_pipeline_options) as p:
        votakvot.extras.beam.inject_trial_into_pipeline(p)
        _ = (p
            | "Initialize" >> beam.Create([tries_per_wi] * work_items).with_output_types(int)
            | "Run trials" >> beam.Map(run_trials)
            | "Summarize"  >> beam.CombineGlobally(combine_results).without_defaults()
            | "ToJSON"     >> beam.Map(serialize_result)
            | "Result"     >> beam_io.WriteToText(output, num_shards=1, shard_name_template="")
             )


def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument("--work_items", type=int, required=True)
    parser.add_argument("--tries_per_wi", type=int, required=True)

    _, req_txt = tempfile.mkstemp()
    req_txt = Path(req_txt)
    req_txt.write_text("\n".join([
        "prometheus-client",
        "google-cloud-monitoring",
        "gcsfs",
    ]))

    opts, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).setup_file = str(
        Path(__file__).resolve().parent.parent / "setup.py"
    )
    pipeline_options.view_as(SetupOptions).requirements_file = str(req_txt)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    temp_location = pipeline_options.view_as(GoogleCloudOptions).temp_location

    votakvot.init(
        # path should begins with "gs://" when used with DataFlow
        path=f"{temp_location}/votakvot",

        hooks=[
            # export metrics to *.jsonl files
            votakvot.extras.prometheus.capture_prometheus_metrics(
                metrics=metrics,
            ),
            # also export metrics to GCM wrom driver & workers
            votakvot.extras.gcm.export_metrics_to_gcm(
                metrics=metrics,
                project_id=pipeline_options.view_as(GoogleCloudOptions).project,
            ),
        ],
    )

    temp_location = pipeline_options.view_as(GoogleCloudOptions).temp_location
    output = f"{temp_location}/estimate_pi_result_{uuid.uuid1().hex}.json"

    run_pipeline(
        output,
        work_items=opts.work_items,
        tries_per_wi=opts.tries_per_wi,
        _pipeline_options=pipeline_options,  # prefixed with '_" - don't include
    )

    print()
    print("PI = ", json.load(FileSystems.open(output)))
    print(votakvot.load_report())


if __name__ == "__main__":
    main()
