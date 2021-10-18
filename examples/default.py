import math
import json
import random
import tempfile

import votakvot


# wrap tracked function with annotation
@votakvot.track()
def calc_pi(n, seed=0):

    # arguments of tracked function are included into report

    random.seed(seed)

    acc = 0
    for i in range(n):
        x = random.random()
        y = random.random()
        acc += x * x + y * y < 1

        # export metrics (csv file)
        votakvot.meter(x=x, y=y)

    pi = 4 * (acc / n)

    # report any information
    votakvot.inform(
        acc=acc,
        delta=abs(math.pi - pi),
    )

    # attach any file to results
    json.dump(
        locals(),
        votakvot.attach("locals.json", 'wt'),  # file-like object
    )

    # return any result
    return pi


def main():

    store_path = tempfile.mkdtemp()
    print("write results into", store_path)

    # enable `votakvot` tracking
    votakvot.init(
        path=store_path,  # '.' by default
    )

    for n in [2 ** i for i in range(4, 15)]:
        print("\nn=", n)
        for s in range(5):

            # just call tracked function
            print(calc_pi(n, s))

    print()

    # load results (as pandas.DataFrame) from current directory
    report = votakvot.load_report()
    print("REPORT:\n", report.to_string())
    print()

    # load more result (as pandas.DataFrame)
    report_ex = votakvot.load_report(full=True)
    print("FULL REPORT - available columns:\n -", "\n - ".join(report_ex.columns))
    print(report_ex.head())

    print("done")


if __name__ == '__main__':
    main()
