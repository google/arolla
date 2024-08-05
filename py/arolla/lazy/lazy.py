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

"""Frontend module for lazy type."""

from __future__ import annotations

import typing as _typing

from arolla import arolla as _arolla
from arolla.lazy import clib as _clib

_M = _arolla.M
_P = _arolla.P
_constraints = _arolla.optools.constraints


@_arolla.optools.add_to_registry()
@_arolla.optools.as_backend_operator(
    'lazy.get_lazy_qtype',
    qtype_constraints=[_constraints.expect_qtype(_P.value_qtype)],
    qtype_inference_expr=_arolla.QTYPE,
)
def get_lazy_qtype(value_qtype):
  """Returns LAZY[value_qtype] qtype."""
  raise NotImplementedError('provided by backend')


@_arolla.optools.add_to_registry()
@_arolla.optools.as_lambda_operator(
    'lazy.is_lazy_qtype',
    qtype_constraints=[_constraints.expect_qtype(_P.qtype)],
)
def is_lazy_qtype(qtype):
  """Returns `present`, if the argument is a "lazy" qtype."""
  return qtype == get_lazy_qtype(_M.qtype.get_value_qtype(qtype))


@_arolla.optools.add_to_registry()
@_arolla.optools.as_backend_operator(
    'lazy.get',
    qtype_constraints=[
        (
            is_lazy_qtype(_P.x),
            f'expected a lazy type, got {_constraints.name_type_msg(_P.x)}',
        ),
    ],
    qtype_inference_expr=_M.qtype.get_value_qtype(_P.x),
)
def get(x):
  """Returns the result of the lazy evaluation."""
  raise NotImplementedError('provided by backend')


class Lazy(_arolla.QValue):
  """Represents an on-demand value.

  NOTE: There is no promise that the value will be cached after the first
  evaluation.
  """

  __slots__ = ()

  @staticmethod
  def from_callable(
      call_fn: _typing.Callable[[], _typing.Any],
      *,
      qtype=_arolla.types.PY_OBJECT,
  ) -> Lazy:
    """Returns a "lazy" object for the result of a function callable invocation.

    The resulting "lazy" object executes the `call_fn` function without
    additional caching or synchronization. If stricter guarantees are needed,
    they must be implemented within the `call_fn` function.

    Args:
      call_fn: A callable object.
      qtype: The returning qtype of `call_fn`.
    """
    if qtype != _arolla.types.PY_OBJECT:
      return _clib.make_lazy_from_callable(qtype, call_fn)

    def wrapper():
      result = call_fn()
      if isinstance(result, _arolla.QValue):
        return result
      return _arolla.types.PyObject(result)

    return _clib.make_lazy_from_callable(qtype, wrapper)

  def get(self) -> _arolla.AnyQValue:
    """Returns the value."""
    return _arolla.abc.invoke_op(get, (self,))


_arolla.abc.register_qvalue_specialization('::arolla::LazyQType', Lazy)


def get_namespaces() -> list[str]:
  """Returns the namespaces for the lazy operators."""
  return ['lazy']
