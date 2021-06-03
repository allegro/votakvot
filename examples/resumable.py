import math
import random
import datetime

import votakvot


# helper function to print the progressbar
def print_progress(i, n):
    if i == n - 1:
        print("\r\033[K", end="")  # clear line
    elif i < n and i % (n // 100) == 0:
        p = i / n
        pb = "=" * int(p * 100)
        print(f"\r{p:3.0%} [{pb:100}]", end="")


# inherit class from helper base class
class resumable_calc_pi(votakvot.resumable_fn):

    # how often class state should be pickled (seconds)
    snapshot_period = datetime.timedelta(seconds=1)

    # or how many iterations should happen between picli g
    # snapshot_each = 100

    def init(self, n, seed):
        self.r = random.Random(seed)
        self.n = n
        self.i = 0
        self.acc = 0

    def loop(self):

        self.i += 1
        print_progress(self.i, self.n)

        # single iteration - state may be pickled
        # in-between invocations of this method

        x, y = self.r.random(), self.r.random()
        self.acc += x ** 2 + y ** 2 < 1

        if self.i < self.n:
            # return None to repeat the loop
            return

        pi = 4 * (self.acc / self.n)
        votakvot.inform(
            pi_diff=abs(math.pi - pi),
        )

        # return any non-None value to break the loop
        return pi


def test():
    # class may be used as regular function!
    calc_pi = resumable_calc_pi.call
    print("TEST", calc_pi(123, 123))


def main():

    print("Try to press Ctrl-C and then rerun this script.")
    votakvot.init(
        path=".",  # path should remain the same for all runs
    )
    n = 10000000

    for s in range(30):
        pit = votakvot.call(
            f"__main__.resumable_pi/n={n}/seed={s}",  # trial ID must to be explicit
            resumable_calc_pi,                        # just pass class instead of function
            {'n': n, 'seed': s},                      # params of resumable_calc_pi.init()
        )
        print(pit.result)


if __name__ == '__main__':
    main()
