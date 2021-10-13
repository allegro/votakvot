from operator import sub
import random
import tempfile
import contextvars
import concurrent.futures

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


class ContextVarExecutor(concurrent.futures.ThreadPoolExecutor):
    def submit(self, fn, *args, **kwargs):
        ctx = contextvars.copy_context()
        return super().submit(ctx.run, fn, *args, **kwargs)


def main():

    store_path = tempfile.mkdtemp()
    print("write results into", store_path)

    votakvot.init(
        path=store_path,
        runner='process',  # run functions inside separate process
    )

    with ContextVarExecutor(max_workers=4) as executor:

        print("sumbit tasks...")
        tasks = []
        for n in [2 ** i for i in range(4, 20)]:
            for s in range(3):
                task = executor.submit(calc_pi, n, s)
                tasks.append(task)

        print("wait tasks...")
        for f in tasks:
            print("pi>", f.result())

    print(">>", votakvot.load_report())
    print("done")


if __name__ == "__main__":
    main()
