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

"""Testing utilities."""

import math
from typing import Any, Callable, Mapping, Sequence

from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types


class PyFloatIsCloseCheck:
  """A predicate checks that two (optional) floats have near values."""

  def __init__(self, *, rtol: float, atol: float):
    """Constructor.

    Args:
      rtol: Relative tolerance.
      atol: Absolute tolerance.
    """
    self._rtol = rtol
    self._atol = atol

  def __call__(
      self, actual_value: float | None, expected_value: float | None
  ) -> str | None:
    """Checks that two (optional) floats have near values.

    Args:
      actual_value: A float or None.
      expected_value: A float or None.

    Returns:
      None iff actual_value is near the expected_value. Otherwise, this
      method returns an error message.
    """
    if actual_value is not None and not isinstance(actual_value, float):
      raise TypeError(
          '`actual_value` must be a real number or None, not '
          + arolla_abc.get_type_name(type(actual_value))
      )
    # Handling for missing values.
    if expected_value is not None and not isinstance(expected_value, float):
      raise TypeError(
          '`expected_value` must be a real number or None, not '
          + arolla_abc.get_type_name(type(expected_value))
      )
    if actual_value is None and expected_value is None:
      return None
    if actual_value is None or expected_value is None:
      return f'not close: actual={actual_value!r}, expected={expected_value!r}'
    if math.isfinite(actual_value) and math.isfinite(expected_value):
      # Handling for regular values.
      dif = abs(actual_value - expected_value)
      tol = self._atol + self._rtol * abs(expected_value)
      if dif > tol:
        return (
            f'not close: actual={actual_value}, expected={expected_value}, '
            f'dif={dif:g}, tol={tol:g}'
        )
      return None
    if actual_value == expected_value:
      return None  # Handling for infinities
    elif math.isnan(actual_value) and math.isnan(expected_value):
      return None  # Handling for NaNs
    return f'not close: actual={actual_value!r}, expected={expected_value!r}'


class PyObjectIsEqualCheck:
  """Checks that two objects are equal.

  NOTE: This check specially handles the following `float` values: 0.0, NaN.

    for x in {0.0, float('nan')}:
      assert x == x
      assert -x == -x
      assert x != -x
  """

  def __call__(self, actual_value: Any, expected_value: Any) -> str | None:
    """Checks that two objects are equal.

    NOTE: This check specially handles the following `float` values: 0.0, NaN.

      for x in {0.0, float('nan')}:
        assert x == x
        assert -x == -x
        assert x != -x

    Args:
      actual_value: A scalar or None.
      expected_value: A scalar or None.

    Returns:
      None iff actual_value == expected_value. Otherwise, this
      method returns an error message.
    """
    if isinstance(actual_value, float) and isinstance(expected_value, float):
      actual_sign = math.copysign(1, actual_value)
      expected_sign = math.copysign(1, expected_value)
      if (actual_sign == expected_sign and actual_value == expected_value) or (
          math.isnan(actual_value) and math.isnan(expected_value)
      ):
        return None
    elif actual_value == expected_value:
      return None
    return f'not equal: actual={actual_value!r}, expected={expected_value!r}'


class QValueIsEqualByFingerprintCheck:
  """Checks that two qvalues are equal by fingerprint."""

  def __call__(
      self, actual_qvalue: arolla_abc.QValue, expected_qvalue: arolla_abc.QValue
  ) -> str | None:
    """Checks that two qvalues are equal by fingerprint.

    Args:
      actual_qvalue: QValue.
      expected_qvalue: QValue.

    Returns:
      None iff actual_qvalue.fingerprint == expected_qvalue.fingerprint.
      Otherwise, this
      method returns an error message.
    """
    if actual_qvalue.fingerprint == expected_qvalue.fingerprint:
      return None
    return (
        f'not equal by fingerprint: actual={actual_qvalue.fingerprint}, '
        f'expected={expected_qvalue.fingerprint}'
    )


_Tolerance = float | Mapping[arolla_abc.QType, float]
_CheckFn = Callable[[Any, Any], str | None]


def _make_allclose_check_predicate(
    scalar_qtype: arolla_abc.QType, *, rtol: _Tolerance, atol: _Tolerance
) -> _CheckFn:
  """Returns an allclose check function corresponding to scalar_qtype."""
  if scalar_qtype not in arolla_types.FLOATING_POINT_QTYPES:
    return PyObjectIsEqualCheck()
  if isinstance(rtol, Mapping):
    rtol = rtol[scalar_qtype]
  if isinstance(atol, Mapping):
    atol = atol[scalar_qtype]
  return PyFloatIsCloseCheck(rtol=rtol, atol=atol)


def _check_sequences(
    actual_py_values: Sequence[Any],
    expected_py_values: Sequence[Any],
    check_fn: _CheckFn,
    *,
    max_error_count: int = 10,
) -> str | None:
  """Checks two sequences, and returns the first n errors."""
  if len(actual_py_values) != len(expected_py_values):
    return (
        'actual_qvalue and expected_qvalue have different lengths:\n  '
        f'actual_len={len(actual_py_values)}, '
        f'expected_len={len(expected_py_values)}'
    )
  errors = []
  for i in range(len(actual_py_values)):
    error = check_fn(actual_py_values[i], expected_py_values[i])
    if error:
      if len(errors) == max_error_count:
        errors.append('...')
        break
      errors.append(f'item[{i}]: {error}')
  if errors:
    return '\n'.join(errors)
  return None


def _check_dict(
    actual_py_values: dict[Any, Any],
    expected_py_values: dict[Any, Any],
    check_fn: _CheckFn,
    *,
    max_error_count: int = 10,
) -> str | None:
  """Checks two dicts, and returns the first n errors."""
  if len(actual_py_values) != len(expected_py_values):
    return (
        'actual_qvalue and expected_qvalue have different lengths:\n  '
        f'actual_len={len(actual_py_values)}, '
        f'expected_len={len(expected_py_values)}'
    )
  errors = []
  for expected_key, expected_value in expected_py_values.items():
    if expected_key not in actual_py_values:
      error = 'missing an expected key'
    else:
      error = check_fn(actual_py_values[expected_key], expected_value)
    if error:
      if len(errors) == max_error_count:
        errors.append('...')
        break
      errors.append(f'item[{expected_key}]: {error}')
  if errors:
    return '\n'.join(errors)
  return None


def assert_qvalue_allclose(
    actual_qvalue: arolla_abc.QValue,
    expected_qvalue: arolla_abc.QValue,
    *,
    rtol: _Tolerance | None = None,
    atol: _Tolerance | None = None,
    msg: str | None = None,
):
  """Arolla variant of NumPy's allclose predicate.

  See the NumPy documentation for numpy.testing.assert_allclose.

  The main difference from the numpy is that assert_qvalue_allclose works with
  qvalues and checks that actual_qvalue and expected_qvalue have the same qtype.
  It also supports sparse array types.

  NOTE: For non-floating-point types this predicate checks the exact equality,
  ignoring `rtol` and `atol`.

  Args:
    actual_qvalue: QValue.
    expected_qvalue: QValue.
    rtol: Relative tolerance. Can be a mapping from scalar_qtype to the
      tolerance value.
    atol: Absolute tolerance. Can be a mapping from scalar_qtype to the
      tolerance value.
    msg: A custom error message.

  Raises:
    AssertionError: If actual_qvalue and expected_qvalue are not close
    up to the given tolerance.
  """
  if rtol is None:
    rtol = {
        arolla_types.FLOAT32: 1e-6,
        arolla_types.FLOAT64: 1e-15,
        arolla_types.WEAK_FLOAT: 1e-15,
    }
  if atol is None:
    atol = 0.0

  if not isinstance(actual_qvalue, arolla_abc.QValue):
    raise TypeError(
        '`actual_value` must be a QValue, not '
        + arolla_abc.get_type_name(type(actual_qvalue))
    )
  if not isinstance(expected_qvalue, arolla_abc.QValue):
    raise TypeError(
        '`expected_value` must be a QValue, not '
        + arolla_abc.get_type_name(type(expected_qvalue))
    )
  if actual_qvalue.qtype != expected_qvalue.qtype:
    raise AssertionError(
        f'QValues not close: {actual_qvalue!r} != {expected_qvalue!r}\n\n  '
        f'actual_qtype={actual_qvalue.qtype}, '
        f'expected_qtype={expected_qvalue.qtype}'
    )
  try:
    scalar_qtype = arolla_types.get_scalar_qtype(actual_qvalue.qtype)
  except ValueError:
    scalar_qtype = None  # QTypes, Operators, Edges, and Tuples. Etc
  if not scalar_qtype and not arolla_types.is_dict_qtype(actual_qvalue.qtype):
    raise TypeError(
        'assert_qvalue_allclose cannot compare the given values: '
        f'{actual_qvalue.qtype} has no corresponding scalar_qtype'
    )
  check_fn = _make_allclose_check_predicate(scalar_qtype, rtol=rtol, atol=atol)
  # NOTE: We have checked qvalue.qtype, and we know that the corresponding
  # qvalue sub-classes have .py_value() methods.
  actual_py_value = actual_qvalue.py_value()  # pytype: disable=attribute-error
  expected_py_value = expected_qvalue.py_value()  # pytype: disable=attribute-error
  if isinstance(actual_py_value, list):
    error = _check_sequences(actual_py_value, expected_py_value, check_fn)
  elif isinstance(expected_py_value, dict):
    error = _check_dict(actual_py_value, expected_py_value, check_fn)
  else:
    error = check_fn(actual_py_value, expected_py_value)
  if error:
    raise AssertionError(
        msg
        or (
            f'QValues not close: {actual_qvalue!r} != {expected_qvalue!r}\n\n  '
            + '\n  '.join(error.split('\n'))
        )
    )


_SUPPORT_ALLEQUAL_BY_FINGERPRINT = frozenset({
    arolla_abc.QTYPE,
    arolla_types.SCALAR_TO_SCALAR_EDGE,
    arolla_types.SCALAR_SHAPE,
    arolla_types.OPTIONAL_SCALAR_SHAPE,
    arolla_types.DENSE_ARRAY_TO_SCALAR_EDGE,
    arolla_types.DENSE_ARRAY_SHAPE,
    arolla_types.ARRAY_TO_SCALAR_EDGE,
    arolla_types.ARRAY_SHAPE,
    arolla_types.make_sequence_qtype(arolla_abc.QTYPE),
    *map(arolla_types.make_sequence_qtype, arolla_types.SCALAR_QTYPES),
    *map(arolla_types.make_sequence_qtype, arolla_types.OPTIONAL_QTYPES),
})


def assert_qvalue_allequal(
    actual_qvalue: arolla_abc.QValue,
    expected_qvalue: arolla_abc.QValue,
    *,
    msg: str | None = None,
):
  """Arolla variant of NumPy's equal predicate.

  See the NumPy documentation for numpy.testing.assert_equal.

  The main difference from the numpy is that assert_qvalue_allequal works with
  qvalues and checks that actual_qvalue and expected_qvalue have the same qtype.
  It also supports sparse array types.

  Args:
    actual_qvalue: QValue.
    expected_qvalue: QValue.
    msg: A custom error message.

  Raises:
    AssertionError: If actual_qvalue and expected_qvalue are not equal.
  """
  if not isinstance(actual_qvalue, arolla_abc.QValue):
    raise TypeError(
        '`actual_value` must be a QValue, not '
        + arolla_abc.get_type_name(type(actual_qvalue))
    )
  if not isinstance(expected_qvalue, arolla_abc.QValue):
    raise TypeError(
        '`expected_value` must be a QValue, not '
        + arolla_abc.get_type_name(type(expected_qvalue))
    )
  if actual_qvalue.qtype != expected_qvalue.qtype:
    raise AssertionError(
        f'QValues not equal: {actual_qvalue!r} != {expected_qvalue!r}\n\n  '
        f'actual_qtype={actual_qvalue.qtype}, '
        f'expected_qtype={expected_qvalue.qtype}'
    )
  try:
    scalar_qtype = arolla_types.get_scalar_qtype(actual_qvalue.qtype)
  except ValueError:
    scalar_qtype = None
  if actual_qvalue.qtype in _SUPPORT_ALLEQUAL_BY_FINGERPRINT:
    error = QValueIsEqualByFingerprintCheck()(actual_qvalue, expected_qvalue)
  elif scalar_qtype or arolla_types.is_dict_qtype(actual_qvalue.qtype):
    # NOTE: We have checked qvalue.qtype, and we know that the corresponding
    # qvalue sub-classes have .py_value() methods.
    actual_py_value = actual_qvalue.py_value()  # pytype: disable=attribute-error
    expected_py_value = expected_qvalue.py_value()  # pytype: disable=attribute-error
    if isinstance(actual_py_value, list):
      error = _check_sequences(
          actual_py_value, expected_py_value, PyObjectIsEqualCheck()
      )
    elif isinstance(expected_py_value, dict):
      error = _check_dict(
          actual_py_value, expected_py_value, PyObjectIsEqualCheck()
      )
    else:
      error = PyObjectIsEqualCheck()(actual_py_value, expected_py_value)
  else:
    raise TypeError(  # Operators, Edges, and Tuples. Etc
        f'assert_qvalue_allequal cannot compare {actual_qvalue.qtype}'
    )
  if error:
    raise AssertionError(
        msg
        or (
            f'QValues not equal: {actual_qvalue!r} != {expected_qvalue!r}\n\n  '
            + '\n  '.join(error.split('\n'))
        )
    )


def assert_qvalue_equal_by_fingerprint(
    actual_qvalue: arolla_abc.QValue,
    expected_qvalue: arolla_abc.QValue,
    *,
    msg: str | None = None,
):
  """Equal-by-fingerprint predicate.

  Args:
    actual_qvalue: QValue.
    expected_qvalue: QValue.
    msg: A custom error message.

  Raises:
    AssertionError: If actual_qvalue.fingerprint and expected_qvalue.fingerprint
      are not equal.
  """
  if not isinstance(actual_qvalue, arolla_abc.QValue):
    raise TypeError(
        '`actual_value` must be a QValue, not '
        + arolla_abc.get_type_name(type(actual_qvalue))
    )
  if not isinstance(expected_qvalue, arolla_abc.QValue):
    raise TypeError(
        '`expected_value` must be a QValue, not '
        + arolla_abc.get_type_name(type(expected_qvalue))
    )
  if actual_qvalue.qtype != expected_qvalue.qtype:
    raise AssertionError(
        f'QValues not equal: {actual_qvalue!r} != {expected_qvalue!r}\n\n  '
        f'actual_qtype={actual_qvalue.qtype}, '
        f'expected_qtype={expected_qvalue.qtype}'
    )
  error = QValueIsEqualByFingerprintCheck()(actual_qvalue, expected_qvalue)
  if error:
    raise AssertionError(
        msg
        or (
            f'QValues not equal: {actual_qvalue!r} != {expected_qvalue!r}\n\n  '
            + '\n  '.join(error.split('\n'))
        )
    )


def assert_expr_equal_by_fingerprint(
    actual_expr: arolla_abc.Expr,
    expected_expr: arolla_abc.Expr,
    *,
    msg: str | None = None,
):
  """Equal-by-fingerprint predicate for expressions.

  Args:
    actual_expr: Expr.
    expected_expr: Expr.
    msg: A custom error message.

  Raises:
    AssertionError: If actual_expr.fingerprint and expected_expr.fingerprint
      are not equal.
  """
  if not isinstance(actual_expr, arolla_abc.Expr):
    raise TypeError(
        '`actual_expr` must be an Expr, not '
        + arolla_abc.get_type_name(type(actual_expr))
    )
  if not isinstance(expected_expr, arolla_abc.Expr):
    raise TypeError(
        '`expected_expr` must be an Expr, not '
        + arolla_abc.get_type_name(type(expected_expr))
    )
  if actual_expr.fingerprint == expected_expr.fingerprint:
    return
  if not msg:
    msg = (
        'Exprs not equal by fingerprint:\n'
        + f'  actual_fingerprint={actual_expr.fingerprint}, '
        + f'expected_fingerprint={expected_expr.fingerprint}\n'
        + '  actual:\n    '
        + '\n    '.join(f'{actual_expr:v}'.split('\n'))
        + '\n  expected:\n    '
        + '\n    '.join(f'{expected_expr:v}'.split('\n'))
    )
  raise AssertionError(msg)
