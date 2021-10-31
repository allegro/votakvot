import functools
import getpass
import logging
import os
import pathlib
import platform
import subprocess
import tempfile

from functools import partial
import typing

from .data import FancyDict, merge_dicts_rec


logger = logging.getLogger(__name__)


class NoMetadataException(Exception):
    pass


def capture_meta(ps=None) -> FancyDict:
    if ps is None:
        ps = providers
    metas = {}
    for key, provider in ps.items():

        logger.info("capture metadata %r", key)
        try:
            m = provider()
        except NoMetadataException:
            continue
        except Exception as e:
            logger.warning("failed to capture %r: %s", key, e)
            continue

        if m is not None:
            sections = key.split(".")
            md = {sections[-1]: m}
            for s in sections[:-1]:
                md = {s: md}
            metas = merge_dicts_rec(metas, md)

    return FancyDict(metas)


# -- meta providers


def _shell_run(cmd, **kwargs):
    try:
        logger.debug("run %r", cmd)
        r = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            check=True,
            **kwargs,
        )
    except subprocess.SubprocessError:
        raise NoMetadataException
    else:
        logger.debug("stderr: %s", r.stderr.decode())
        return r.stdout.decode().strip()


def _get_meta_git_treeish():
    repo = _shell_run("git rev-parse --show-toplevel")
    indexf = pathlib.Path(repo) / ".git" / "index"

    with tempfile.NamedTemporaryFile(buffering=0) as tf:
        tf.write(indexf.read_bytes())
        env = {**os.environ, "GIT_INDEX_FILE": tf.name}
        _shell_run("git add -u", env=env)
        return _shell_run("git write-tree", env=env)


def memoize(f):
    return functools.lru_cache(None)(f)


providers: typing.Dict[str, typing.Callable[[], str]] = {}

providers['system.platform'] = memoize(platform.platform)
providers['system.user'] = memoize(getpass.getuser)
providers['system.node'] = memoize(platform.node)
providers['system.cwd'] = os.getcwd

providers['process.pid'] = memoize(os.getpid)
providers['process.gid'] = memoize(os.getgid)

providers['git.repo'] = memoize(partial(_shell_run, "git rev-parse --show-toplevel"))
providers['git.describe'] = memoize(partial(_shell_run, "git describe --dirty --tags --long --always"))
providers['git.branch'] = memoize(partial(_shell_run, "git branch --show-current"))
providers['git.commit'] = memoize(partial(_shell_run, "git rev-parse HEAD"))
providers['git.treeish'] = memoize(_get_meta_git_treeish)

providers['python.version'] = memoize(platform.python_version)
providers['python.venv'] = memoize(partial(os.environ.get, "VIRTUAL_ENV"))

# FIXME: Enable by default?
# providers['python.pip_freeze'] = lambda: _shell_run("pip freeze").splitlines()
