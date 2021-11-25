import dataclasses
import io
import logging
import urllib.parse

from functools import wraps
from typing import Any, Dict, Mapping, NamedTuple

import fsspec
import wrapt
import yaml
import yaml.constructor


logger = logging.getLogger(__name__)


def path_fs(path: str) -> fsspec.AbstractFileSystem:
    scheme = urllib.parse.urlparse(path).scheme or "file"
    return fsspec.filesystem(scheme, auto_mkdir=True)


class AutoCommitableFileWrapper(wrapt.ObjectProxy):

    def __exit__(self, *args, **kwargs):
        return super().__exit__(*args, **kwargs)

    def close(self):
        f = self.__wrapped__
        if isinstance(f, io.TextIOWrapper):
            f = f.buffer

        self.__wrapped__.close()
        if not f.autocommit:
            logger.debug("commit file %s", f)
            f.commit()

    def __del__(self):
        logger.info("close garbage-collected %s", self)
        self.close()


class FancyDict(dict):
    def __getattr__(self, key):
        if key.startswith("__"):
            return dict.__getattr__(self, key)
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, key):
        del self[key]

    def __repr__(self):
        y = yaml.dump({"yaml": self}, Dumper=YAMLDumper)
        assert y.startswith("yaml:")
        return f"<yaml{y[5:]}>"


class BadPythonYAML(NamedTuple):
    tag: str
    value: Any


class YAMLDumper(yaml.Dumper):

    def __init__(self, *args, **kwargs):
        kwargs['indent'] = 4
        kwargs['sort_keys'] = False
        return yaml.Dumper.__init__(self, *args, **kwargs)

    def write_line_break(self, data=None):
        super().write_line_break(data)
        if len(self.indents) == 1:
            super().write_line_break()

    def represent_bad_python_ref(self, data):
        return self.represent_scalar(data.tag, data.value)


class YAMLLoader(yaml.FullLoader):

    def construct_yaml_map(self, node):
        data = FancyDict()
        yield data
        value = self.construct_mapping(node)
        data.update(value)

    def _catch_bad_python_yaml(f):

        @wraps(f)
        def method(self, suffix, node):
            try:
                return f(self, suffix, node)
            except yaml.constructor.ConstructorError:
                return BadPythonYAML(node.tag, node.value)

        return method

    construct_python_name = _catch_bad_python_yaml(yaml.FullLoader.construct_python_name)
    construct_python_module = _catch_bad_python_yaml(yaml.FullLoader.construct_python_module)
    construct_python_object = _catch_bad_python_yaml(yaml.FullLoader.construct_python_object)
    construct_python_object_apply = _catch_bad_python_yaml(yaml.FullLoader.construct_python_object_apply)
    construct_python_object_new = _catch_bad_python_yaml(yaml.FullLoader.construct_python_object_new)


YAMLDumper.add_representer(
    FancyDict,
    YAMLDumper.represent_dict)

YAMLDumper.add_representer(
    BadPythonYAML,
    YAMLDumper.represent_bad_python_ref)

YAMLLoader.add_constructor(
    "tag:yaml.org,2002:map",
    YAMLLoader.construct_yaml_map)

YAMLLoader.add_multi_constructor(
    'tag:yaml.org,2002:python/module:',
    YAMLLoader.construct_python_module)

YAMLLoader.add_multi_constructor(
    'tag:yaml.org,2002:python/object:',
    YAMLLoader.construct_python_object)

YAMLLoader.add_multi_constructor(
    'tag:yaml.org,2002:python/object/new:',
    YAMLLoader.construct_python_object_new)

YAMLLoader.add_multi_constructor(
    'tag:yaml.org,2002:python/object/apply:',
    YAMLLoader.construct_python_object_apply)

YAMLLoader.add_multi_constructor(
    'tag:yaml.org,2002:python/name:',
    YAMLLoader.construct_python_name)


def load_yaml_file(file: io.IOBase) -> Any:
    return yaml.load(file, Loader=YAMLLoader)


def dump_yaml_file(file: io.IOBase, data):
    yaml.dump(data, file, Dumper=YAMLDumper)


def _plainify_dict_rec(d, res, prefix):

    if dataclasses.is_dataclass(d):
        d = dataclasses.asdict(d)
    elif not isinstance(d, Mapping):
        return d

    for k, v in d.items():
        if isinstance(v, Mapping) or dataclasses.is_dataclass(v):
            _plainify_dict_rec(v, res, prefix=(prefix + k + "."))
        else:
            res[prefix + k] = v


def plainify_dict(d: Dict) -> Dict:
    res = FancyDict()
    _plainify_dict_rec(d, res, "")
    return res


def maybe_plainify(value: Any, singletone_key: str = "value") -> Dict:
    if value is None:
        return {}
    elif isinstance(value, Mapping) or dataclasses.is_dataclass(value):
        return plainify_dict(value)
    else:
        return {singletone_key: value}


def merge_dicts_rec(a: Any, b: Any) -> Dict:
    if isinstance(a, Dict) and isinstance(b, Dict):
        return FancyDict({
            **a, **b,
            **{k: merge_dicts_rec(a[k], b[k]) for k in a.keys() & b.keys()},
        })
    elif isinstance(b, Dict):
        return FancyDict(b)
    else:
        return b
