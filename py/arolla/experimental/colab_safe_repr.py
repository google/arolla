# Copyright 2025 Google LLC
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

"""An experimental extension to display Arolla values in operator signatures.

Google Colab renders only a limited set of types in function signatures during
interactive mode because custom type rendering can be slow.

This experimental extension introduces a special class attribute,
_COLAB_HAS_SAFE_REPR. When set to True, it enables the use of repr() in
google.colab._inspector._safe_repr.

For compatibility with existing rendering, repr() for a type should be fast and
produce a concise, single-line string.
"""

import sys
from typing import Any, Callable

_MODULE_NAME = 'google.colab._inspector'


_SafeReprFn = Callable[..., str | None]


def _exchange_safe_repr(new_safe_repr: _SafeReprFn) -> _SafeReprFn:
  """An extension point for the unit-testing."""
  module = sys.modules.get(_MODULE_NAME, None)
  if module is None:
    raise ImportError(f'{_MODULE_NAME} is not loaded yet')
  result = module._safe_repr  # pylint: disable=protected-access
  module._safe_repr = new_safe_repr  # pylint: disable=protected-access
  return result


_original_safe_repr: _SafeReprFn | None = None


def _impl(obj: Any, *args: Any, **kwargs: Any) -> str | None:
  if getattr(type(obj), '_COLAB_HAS_SAFE_REPR', None):
    return repr(obj)
  assert _original_safe_repr is not None
  return _original_safe_repr(obj, *args, **kwargs)


def enable():
  """Enables the extension."""
  global _original_safe_repr
  if _original_safe_repr is not None:
    _exchange_safe_repr(_original_safe_repr)
  _original_safe_repr = _exchange_safe_repr(_impl)


def disable():
  """Disables the extension."""
  global _original_safe_repr
  if _original_safe_repr is not None:
    _exchange_safe_repr(_original_safe_repr)
    _original_safe_repr = None


enable()
