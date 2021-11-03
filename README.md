votakvot
========

A simple tool helping to track information, metrics, and files during
code development, testing, probing, experimentation and analysis.


The idea
--------

You write Python code, annotate a function, call it.

*Votakvot* track what function parameters are, its result, git repo status, etc...

Change your code, change parameters, try to rerun the function, experiment.

Then *votakvot* may load back all information as pandas `DataFrame`.

Play with data and find the best combination of function parameters and version of a source code.


Basic usage
-----------

Write a function and wrap it with an annotation `votakvot.track`:

```python
@votakvot.track()
def my_experiment(one, two):
    print(one, two)
    return one + two
```

Then call `votakvot.init()` to initialize library internals:

```python
votakvot.init(
    path="./my-results",  # path, where to store results, "." by default
)
```

Now any invocation of `my_experiment(...)` creates a new unique folder
inside *./my-results*. That new subfolder contains a file
*votakvot.yaml* with:

- globally unique id (uuid4)
- timestamps (created, started, finished, duration)
- function parameters
- function result
- git info (branch, commit, work directory tree-ish)
- system information (machine, user, python version)
- traceback text on exception
- any additional ad-hoc information

Additional information added with `votakvot.inform`:

```python
@votakvot.track()
def my_experiment(one, two):
    ...
    votakvot.inform(
        any_custom_field="any-value",
        other_custom_field=["structured", "data"],
    )
    ...
```

Please note that any parameter, returned, or informed value is serialized
by [pyyaml](https://pyyaml.org/wiki/PyYAMLDocumentation). It supports
all standard python types: int, float, complex, bool, str, bytes, list,
dict, tuple, set, datetime, None. Also any pickleable python class may
be serialized (including namedtuples and dataclasses), however it is not
recommended.

Load reports
------------

Content of multiple *votakvot.yaml* files can be loaded into
`pandas.DataFrame` by using function `votakvot.load_report()`. It gets
file path as a first argument. A path may be prefixed with a protocol
(`ftp://`, `ssh://` etc).
Some prefixes (like `gs://` or `s3://`) may require extra libraries to be installed (see
[fsspec protocols](https://filesystem-spec.readthedocs.io/en/latest/?badge=latest#implementations)
for details).

Also path may contain glob patterns: `*` corresponds to any string
without `/`, `**` corresponds to anything.

By default `@votakvot.track()`
adds date-time of invocation into a subdirectory name:

``` {.sourceCode .}
{function module} / {function name} / {yy}-{mm}-{dd} / {hour}:{minute}:{second} / {unique uuid}
```

This allows loading results only for a particular module, function, date
or date-time only:

```python
root = "/path/to/directory/with/results"

# load all experiments from `root`
votakvot.load_report(root)

# load all experiments with additinoal fields
votakvot.load_report(root, full=True)

# load all experiemnts for specified function only
votakvot.load_report(f"{root}/my_module/function_name")

# load experiments for a single day 2021-05-20 (any function)
votakvot.load_report(f"{root}/**/21-05-20")

# load exprriments for a particular hour (any function)
votakvot.load_report(f"{root}/**/21-05-20/15:*")
```

A few dataframes may be merged with \`pandas.concat\`:

```python
# load results for 3 days
df = pandas.concat(
    votakvot.load_report(f"{root}/**/{day}/**")
    for day in ["21-05-20", "21-05-21", "21-05-22"]
)
```

Result `DataFrame` can be filtered, sorted, updated, transformed,
plotted, serialized, and analyzed with all power of Pandas. See
[pandasttutorial](https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html).

Additionally *raw* information may be obtained with `load_trials`
function:

```python
# load dict of {id -> votakvot.Trial}
vs = load_trials(root)

print("count", len(vs))
print("ids:", vs.keys())

# print raw content of `votakvot.yaml` files
print("data", [v.data for v in vs.values()])

# print only git related information
print("git commits", [v.meta.git.commit for v in vs.values()])
```

Metrics
-------

Tracked function may produce metrics:

```python
votakvot.meter(
    metric_name="metric value",
)
```

Metrics are stored as series of csv files and can be loaded to single `pandas.DataFrame`:

```python
rep = votakvot.load_report()
tid = rep.loc[0]['tid']             # trial id
votakvot.metrics.load_metrics(tid)  # instance of pd.DataFrame
```

Attached files
--------------

A regular file may be created next to *votakvot.yaml*. Use this to
store debug information (traceback, logs), create artifacts or even
store intermediate results of computation (see resumable tasks).

```python
@votakvot.track()
def my_experiment(one, two):
    ...
    with votakvot.attach("my-file-name.txt", mode='tw') as f:
        f.write("some text ...")
```

Metadata
--------

Library automatically adds metadata to all generated *votakvot.yaml* files.
Metadata includes information about python environment, git repo
(commit, branch, and index hash), OS version. You can add extra
metadata by putting values into dictionary `votakvot.meta.providers`:

```python
# make copy of all providers
my_proivders = dict(votakvot.meta.providers)

# add information about k8s
my_proivders['k8s.version'] = lambda: subprocess.getoutput("kubectl version")
my_proivders['k8s.cluster_info'] = lambda: subprocess.getoutput("kubectl cluster-info")

# include list of all python libraries
my_proivders['python.pip_freeze'] = lambda: subprocess.getoutput("pip freeze")

# delete deafult medatata for 'git'
my_proivders = {
    k: v
    for k, v in votakvot.metadata_providers.items()
    if not k.startswith("git.")
}

# here 'kubectl' command is invoked, but 'git' does not
votakvot.init(
    meta_providers=my_proivders,  # use custom set of meta providers
)
```

Resumable tasks
---------------

Some trials may take a lot of time to complete, it is possible to
make them resumable. A tracked function may be refactored into an iterable
pickleable object. If the program fails (or terminated manually) a pickled object
is still left on the disk and *votakvot* will automatically loaded it during the next trial run.

```python
class my_function(votakvot.resumable_fn):

    snapshot_period = 5  # snapshot each 5sec, only in-between `self.loop()` calls

    def init(self, one, two):
        self.one = one
        ...

    def loop(self):
        if ...:
            return "result"  # non-None value to finish compution
        else:
            return None      # repeat `self.loop()` one more time

# autoresume when there is a snapshot for this id on the filesystem
votakvot.run(
    f"resumable_pi/n={n}/seed={s}",   # id must be explicitly specified for resumable tasks
    my_function,
    one=1,
    two=2,
)
```

votakvot-ab
-----------

Votakvot comes with basic benchmarking utility `votakvot-ab`.
It behaves similar to known [ab](http://www.skrenta.com/rt/man/ab.8.html) utility,
but instead of making HTTP calls invokes user provided python callback.

Utility patch socker library with [gevent](http://www.gevent.org/), this allows to
run IO-bounded code with bigger concurrency.

Given file `my_module.py`
```python
import requests
import requests.adapters

session = requests.Session()
session.mount('http://', requests.adapters.HTTPAdapter(pool_maxsize=100))

def get_example(domain="org"):
   return session.get(f"http://example.{domain}/").status_code
```

call function 1000 times in 10 "threads" (using greenlets):
```bash
votakvot-ab -n1000 -c10 my_module.get_example domain=com
```

See `votakvot-ab --help` for all parameters.

## License

*Votakvot* is released under the Apache 2.0 license (see [LICENSE](LICENSE))
