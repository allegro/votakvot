import random
import tempfile
import math

import votakvot


@votakvot.track()
def calc_pi(n):
    acc = 0
    for _ in range(n):
        x = random.random()
        y = random.random()
        acc += x * x + y * y < 1
    pi = 4 * (acc / n)
    votakvot.inform(
        acc=acc,
        delta=abs(math.pi - pi),
    )
    return pi


def main():
    votakvot.init(
        path=tempfile.mkdtemp(),  # "." by default
    )

    for x in range(2, 7):
        n = 10 ** x
        pi = calc_pi(n)
        print(f"n=10**{x} >> pi={pi}")

    r = votakvot.load_report()
    print(r.to_string())


if __name__ == '__main__':
    main()
