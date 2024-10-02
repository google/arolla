# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helper functions."""

import sys
import types
import typing

from arolla.abc import clib
from arolla.abc import dummy_numpy

vectorcall = clib.vectorcall


def get_type_name(t: typing.Type[typing.Any]) -> str:
  """Returns a meaningful type name."""
  if t.__module__ == "builtins":
    return t.__qualname__
  return "{}.{}".format(t.__module__, t.__qualname__)


# A registry for cache cleaning facilities.
#
# You can register your function in arolla.abc.clear_cache_callbacks if you want
# to be notified when your caches should be cleared.
#
CacheClearCallback = typing.Callable[[], None]
cache_clear_callbacks: set[CacheClearCallback] = set()


def clear_caches() -> None:
  """Invokes all the registered cache_clear callbacks."""
  for callback in cache_clear_callbacks:
    callback()


def import_numpy() -> types.ModuleType:
  """Returns the `numpy` module if available, else raises ImportError.

  This function can help if you want to avoid importing `numpy` at the top of
  the file or if you don't want to list NumPy in the build dependencies.

  It behaves like `import numpy`. If the module is unavailable, the function
  raises ImportError.
  """
  import numpy  # pylint: disable=g-import-not-at-top  # pytype: disable=import-error

  return numpy


def get_numpy_module_or_dummy() -> types.ModuleType:
  """Returns the `numpy` module if imported, else returns a dummy module.

  This function helps to avoid a dependency on NumPy if you only need to
  check if a value is one of the numpy types. Example:

    m = arolla.abc.get_numpy_module_or_dummy()
    if isinstance(value, m.ndarray):
      ...

  -- if NumPy has not been imported yet, `m.ndarray` is just a dummy type.

  The idea is that you can get the module from the `sys.modules` dictionary if
  it has been already imported; and otherwise there cannot be any NumPy scalars
  or arrays around.
  """
  result = sys.modules.get("numpy")
  try:
    result.__version__  # pytype: disable=attribute-error
  except AttributeError:
    # The `numpy` module is not imported (or in a partially initialized state).
    result = dummy_numpy
  return result  # pytype: disable=bad-return-type
