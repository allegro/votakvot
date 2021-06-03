import random
import tempfile
import prometheus_client as pc

import votakvot
from votakvot.extras.gcm import export_metrics_to_gcm


counter = pc.Counter("ops", "Number of operations")


@votakvot.track()
def calc_pi(n):

    acc = 0
    for i in range(n):
        x = random.random()
        y = random.random()
        acc += x * x + y * y < 1
        counter.inc()

    return 4 * (acc / n)


def main():
    store_path = tempfile.mkdtemp()
    print("write results into", store_path)

    votakvot.init(
        path=store_path,
        runner='process',
        hooks=[
            export_metrics_to_gcm(
                # XXX: project_id="my-project",
                metrics=[
                    counter,
                ],
            ),
        ],
    )

    phi = (1 + 5 ** 0.5) / 2
    for x in range(2, 50):
        n = int(phi ** x)
        pi = calc_pi(n)
        print(f"n={n} >> pi={pi}")

    print(votakvot.load_report())


if __name__ == '__main__':
    main()
