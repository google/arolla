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

"""(Private) Casting utils for qtype/qvalue.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

import functools
from typing import Callable, Iterable

from arolla.abc import abc as rl_abc


class QTypeError(ValueError):
  """A specialised error for QType casting."""

  pass


# Precompiled 'qtype.common_qtype' operator.
_compiled_expr_common_qtype: Callable[
    [rl_abc.QType, rl_abc.QType], rl_abc.QType
] = rl_abc.CompiledExpr(
    rl_abc.unsafe_parse_sexpr(
        ('qtype.common_qtype', rl_abc.leaf('x'), rl_abc.leaf('y'))
    ),
    dict(x=rl_abc.QTYPE, y=rl_abc.QTYPE),
)


# Precompiled 'qtype.broadcast_qtype_like' operator.
_compiled_expr_broadcast_qtype_like: Callable[
    [rl_abc.QType, rl_abc.QType], rl_abc.QType
] = rl_abc.CompiledExpr(
    rl_abc.unsafe_parse_sexpr((
        'qtype.broadcast_qtype_like',
        rl_abc.leaf('target'),
        rl_abc.leaf('x'),
    )),
    dict(target=rl_abc.QTYPE, x=rl_abc.QTYPE),
)

# Precompiled 'qtype.get_shape_qtype' operator.
_compiled_expr_get_shape_qtype: Callable[[rl_abc.QType], rl_abc.QType] = (
    rl_abc.CompiledExpr(
        rl_abc.unsafe_parse_sexpr(('qtype.get_shape_qtype', rl_abc.leaf('x'))),
        dict(x=rl_abc.QTYPE),
    )
)


def common_qtype(*qtypes: rl_abc.QType) -> rl_abc.QType:
  """Returns a common type that the given types can be implicitly cast to.

  Examples:

    common_qtype:
      INT32                  -> INT32
      INT32, INT32           -> INT32
      INT32, INT64           -> INT64
      OPTIONAL_INT32, INT64  -> OPTIONAL_INT64
      OPTIONAL_INT32, ARRAY_INT64
                             -> ARRAY_INT64

      OPTIONAL_INT64, DENSE_ARRAY_INT32
                             -> DENSE_ARRAY_INT64

      OPTIONAL_INT32, ARRAY_INT32, INT64
                             -> ARRAY_INT64

      INT32, BOOLEAN         -> QTypeError
      INT32, FLOAT32         -> QTypeError
      ARRAY_INT32, DENSE_ARRAY_INT32
                             -> QTypeError

  Args:
    *qtypes: QTypes.

  Returns:
    A common type for the given types.

  Raises:
    QTypeError: When no common type exists.
  """
  if not qtypes:
    raise TypeError('expected at least one input qtype')
  for i, qtype in enumerate(qtypes):
    if not isinstance(qtype, rl_abc.QType):
      raise TypeError(
          f'expected all QTypes, got qtypes[{i}]:'
          f' {rl_abc.get_type_name(type(qtype))}'
      )
  unique_qtypes = set(qtypes)
  if len(unique_qtypes) == 1:
    return qtypes[0]
  result = functools.reduce(_compiled_expr_common_qtype, unique_qtypes)
  if result == rl_abc.NOTHING:
    raise QTypeError('no common qtype for: ' + ', '.join(map(str, qtypes)))
  return result


def broadcast_qtype(
    target_qtypes: Iterable[rl_abc.QType], qtype: rl_abc.QType
) -> rl_abc.QType:
  """Returns `qtype` after broadcasting using a common shape of target qtypes.

  Examples:

    broadcast_qtypes:
      [], INT32                 -> INT32
      [OPTIONAL_FLOAT32], INT32 -> OPTIONAL_INT32

      [OPTIONAL_FLOAT32, ARRAY_BYTES], INT32
                                -> ARRAY_INT32

      [], EMPTY_TUPLE           -> QTypeError

      [ARRAY_INT32], DENSE_ARRAY_INT32
                                -> QTypeError
  Args:
    target_qtypes: Target qtypes.
    qtype: A qtype for broadcasting.

  Returns:
    The `qtype` after broadcasting to a common shape.

  Raises:
    QTypeError: When no broadcasting possible.
  """
  for i, target_qtype in enumerate(target_qtypes):
    if not isinstance(target_qtype, rl_abc.QType):
      raise TypeError(
          f'expected all QTypes, got target_qtypes[{i}]:'
          f' {rl_abc.get_type_name(type(target_qtype))}'
      )
  if not isinstance(qtype, rl_abc.QType):
    raise TypeError(
        f'expected QType, got qtype: {rl_abc.get_type_name(type(qtype))}'
    )
  unique_target_qtypes = set(target_qtypes)
  if unique_target_qtypes:
    result = qtype
    for target_qtype in unique_target_qtypes:
      result = _compiled_expr_broadcast_qtype_like(target_qtype, result)
  else:
    # Test whether the given qtype supports broadcasting at all.
    result = _compiled_expr_broadcast_qtype_like(qtype, qtype)
  if result == rl_abc.NOTHING:
    raise QTypeError(
        'QType broadcasting failed: ['
        + ', '.join(map(str, target_qtypes))
        + f'], {qtype}'
    )
  return result


def get_shape_qtype_or_nothing(qtype: rl_abc.QType) -> rl_abc.QType:
  """Returns shape qtype corresponding to the given `qtype`.

  Examples:

    get_scalar_qtype:
      INT32            -> SCALAR_SHAPE
      OPTIONAL_FLOAT32 -> OPTIONAL_SCALAR_SHAPE
      ARRAY_BYTES      -> ARRAY_SHAPE
      TUPLE            -> NOTHING

  Args:
    qtype: A qtype.

  Returns:
    The shape qtype corresponding to the given `qtype`.
  """
  if not isinstance(qtype, rl_abc.QType):
    raise TypeError(
        f'expected QType, got qtype: {rl_abc.get_type_name(type(qtype))}'
    )
  return _compiled_expr_get_shape_qtype(qtype)
