import random
import tempfile

import votakvot
from votakvot.extras.prometheus import capture_prometheus_metrics

import prometheus_client as pc


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
            capture_prometheus_metrics(
                period=0.5,
                metrics=[counter],
            ),
        ],
    )

    phi = (1 + 5 ** 0.5) / 2
    for x in range(5, 40):
        n = int(phi ** x)
        pi = calc_pi(n)
        print(f"n={n} >> pi={pi}")

    r = votakvot.load_report()
    print(r.to_string())


if __name__ == '__main__':
    main()
