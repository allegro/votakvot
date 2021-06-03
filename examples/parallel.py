import random
import tempfile

import votakvot


@votakvot.track()
def calc_pi(n, seed=0):
    random.seed(seed)
    acc = 0
    for i in range(n):
        x = random.random()
        y = random.random()
        acc += x * x + y * y < 1
    pi = 4 * (acc / n)
    return pi


def main():

    store_path = tempfile.mkdtemp()
    print("write results into", store_path)

    votakvot.init(
        runner='process',
        path=store_path,
    )

    pits = calc_pi.multi([
        {'n': n, 'seed': s}
        for n in [2 ** i for i in range(5, 20)]
        for s in range(30)
    ])
    for t in pits:
        print("pi>", dict(t.params), t.result)

    print("done")


if __name__ == "__main__":
    main()
