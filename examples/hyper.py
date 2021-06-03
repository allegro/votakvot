# flake8: noqa
# example is copied from hyperopt docs
# added lines are marked with `# votakvot`

# pip install 'hyperopt'

import votakvot   # votakvot
import tempfile

# define an objective function
@votakvot.track()  # votakvot
def objective(args):
    case, val = args
    votakvot.inform(val=val, case=case)  # votakvot
    if case == 'case 1':
        return val
    else:
        return val ** 2


# define a search space
from hyperopt import hp
space = hp.choice('a',
    [
        ('case 1', 1 + hp.lognormal('c1', 0, 1)),
        ('case 2', hp.uniform('c2', -10, 10))
    ])

store_path = tempfile.mkdtemp()
print("write results into", store_path)
votakvot.init(path=store_path)  # votakvot

# minimize the objective over the space
from hyperopt import fmin, tpe, space_eval
best = fmin(objective, space, algo=tpe.suggest, max_evals=100)

print(best)
# -> {'a': 1, 'c2': 0.01420615366247227}
print(space_eval(space, best))
# -> ('case 2', 0.01420615366247227}

print(votakvot.load_report()[['val', 'case', 'result']])  # votakvot
