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

Please avoid using this module directly. Use arolla.rl (preferrably) or
arolla.types.types instead.
"""

import functools
from typing import Callable, Iterable

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import clib


class QTypeError(ValueError):
  """A specialised error for QType casting."""

  pass


# Precompiled 'qtype.common_qtype' operator.
_compiled_expr_common_qtype: Callable[
    [arolla_abc.QType, arolla_abc.QType], arolla_abc.QType
] = arolla_abc.CompiledExpr(
    arolla_abc.unsafe_parse_sexpr(
        ('qtype.common_qtype', arolla_abc.leaf('x'), arolla_abc.leaf('y'))
    ),
    dict(x=arolla_abc.QTYPE, y=arolla_abc.QTYPE),
)


# Precompiled 'qtype.broadcast_qtype_like' operator.
_compiled_expr_broadcast_qtype_like: Callable[
    [arolla_abc.QType, arolla_abc.QType], arolla_abc.QType
] = arolla_abc.CompiledExpr(
    arolla_abc.unsafe_parse_sexpr((
        'qtype.broadcast_qtype_like',
        arolla_abc.leaf('target'),
        arolla_abc.leaf('x'),
    )),
    dict(target=arolla_abc.QTYPE, x=arolla_abc.QTYPE),
)

# Precompiled 'qtype.get_shape_qtype' operator.
_compiled_expr_get_shape_qtype: Callable[
    [arolla_abc.QType], arolla_abc.QType
] = arolla_abc.CompiledExpr(
    arolla_abc.unsafe_parse_sexpr(
        ('qtype.get_shape_qtype', arolla_abc.leaf('x'))
    ),
    dict(x=arolla_abc.QTYPE),
)


def common_qtype(*qtypes: arolla_abc.QType) -> arolla_abc.QType:
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
      INT32, BYTES           -> QTypeError
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
    if not isinstance(qtype, arolla_abc.QType):
      raise TypeError(
          f'expected all QTypes, got qtypes[{i}]:'
          f' {arolla_abc.get_type_name(type(qtype))}'
      )
  unique_qtypes = set(qtypes)
  if len(unique_qtypes) == 1:
    return qtypes[0]
  result = functools.reduce(_compiled_expr_common_qtype, unique_qtypes)
  if result == arolla_abc.NOTHING:
    raise QTypeError('no common qtype for: ' + ', '.join(map(str, qtypes)))
  return result


# We redefine scalar_qtype.WEAK_FLOAT here to avoid a dependency cycle.
_WEAK_FLOAT = clib.weak_float(0).qtype


def common_float_qtype(*qtypes: arolla_abc.QType) -> arolla_abc.QType:
  """Returns a common floating point type that the given type(s) can be implicitly cast to.

  Examples:
      INT32                  -> FLOAT32
      INT32, INT64           -> FLOAT32
      INT32, FLOAT64         -> FLOAT64
      INT32, WEAK_FLOAT      -> FLOAT32
      WEAK_FLOAT, WEAK_FLOAT -> FLOAT32

      BYTES                  -> QTypeError
      INT32, BOOLEAN         -> QTypeError

      See also examples in `common_qtype`.

  Args:
    *qtypes: QTypes.

  Returns:
    A common floating point type for the given types.

  Raises:
    QTypeError: When any of the types is not implicitly castable to float or no
      common type exists.
  """
  if not qtypes:
    raise TypeError('expected at least one input qtype')
  return common_qtype(*qtypes, _WEAK_FLOAT)


def broadcast_qtype(
    target_qtypes: Iterable[arolla_abc.QType], qtype: arolla_abc.QType
) -> arolla_abc.QType:
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
    if not isinstance(target_qtype, arolla_abc.QType):
      raise TypeError(
          f'expected all QTypes, got target_qtypes[{i}]:'
          f' {arolla_abc.get_type_name(type(target_qtype))}'
      )
  if not isinstance(qtype, arolla_abc.QType):
    raise TypeError(
        f'expected QType, got qtype: {arolla_abc.get_type_name(type(qtype))}'
    )
  unique_target_qtypes = set(target_qtypes)
  if unique_target_qtypes:
    result = qtype
    for target_qtype in unique_target_qtypes:
      result = _compiled_expr_broadcast_qtype_like(target_qtype, result)
  else:
    # Test whether the given qtype supports broadcasting at all.
    result = _compiled_expr_broadcast_qtype_like(qtype, qtype)
  if result == arolla_abc.NOTHING:
    raise QTypeError(
        'QType broadcasting failed: ['
        + ', '.join(map(str, target_qtypes))
        + f'], {qtype}'
    )
  return result


def get_shape_qtype_or_nothing(qtype: arolla_abc.QType) -> arolla_abc.QType:
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
  if not isinstance(qtype, arolla_abc.QType):
    raise TypeError(
        f'expected QType, got qtype: {arolla_abc.get_type_name(type(qtype))}'
    )
  return _compiled_expr_get_shape_qtype(qtype)
