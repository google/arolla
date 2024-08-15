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

"""Utilities for unit-tests for pointwise operators."""

import functools
import itertools
import math
import struct

from arolla import arolla

SCALAR_QTYPE_MUTATORS = (
    lambda x: x,
    arolla.make_optional_qtype,
)

ARRAY_QTYPE_MUTATORS = (
    arolla.make_dense_array_qtype,
    arolla.make_array_qtype,
)

ALL_QTYPE_MUTATORS = SCALAR_QTYPE_MUTATORS + ARRAY_QTYPE_MUTATORS


class IntOnly(int):
  """A special marker for an int-only value for gen_case().

  Function `gen_case()` treats an IntOnly as int, but avoids casting it to
  float. It's convenient for some operator tests. In particular, for
  math.floordiv and math.mod operators:

    0. // -1. == -0.
    0  // -1  == -0

    0. % -1.  == -0.
    0  % -1   == -0

  With IntOnly, the corresponding gen_cases call may look like:

    gen_cases(
        (
          (0., -1. -0.),
          (IntOnly(0), -1, 0),
        ),
        (FLOAT32, FLOAT32, FLOAT32),
        (INT32, INT32, INT32),
    ) = (
        (float32(0.), float32(-1.), float32(-0.)),
        (int32(0), int32(-1), int32(0)),
    )
  """

  pass


def _validate_item(x):
  """Validates a row item.

  This function checks that the given value is one of supported types:
    * None
    * python scalar value: bool, bytes, string, int float
    * arolla scalar value: UNIT, BYTES, TEXT, INTnn, FLOATnn
    * arolla qtype

  Args:
    x: Item value.

  Returns:
    Nothing if `x` is a valid item, and raises ValueError otherwise.
  """
  if x is None or isinstance(x, (bool, bytes, str, int, float)):
    # NOTE: IntOnly is a subclass of int, so no explicit handling is needed.
    return True
  if isinstance(x, arolla.QValue):
    if x.qtype in arolla.types.SCALAR_QTYPES or x.qtype == arolla.QTYPE:
      return True
    error_trailer = (
        f' Actually got {arolla.abc.get_type_name(type(x))}, qtype={x.qtype}'
    )
  else:
    error_trailer = f' Actually got {arolla.abc.get_type_name(type(x))}'
  raise ValueError(
      'Expected row items to be None, bool, bytes, str, int, float,'
      ' IntOnly, QType, or a scalar QValue.'
      + error_trailer
  )


def _validate_row(row):
  """Validates a test_data row.

  This function checks that row is a tuple with the expected number of items,
  and all items are scalars (or None).

  Args:
    row: A tuple with items.
  """
  if not isinstance(row, tuple):
    raise ValueError(
        f'Expected rows to be tuples, got {arolla.abc.get_type_name(type(row))}'
    )
  for x in row:
    _validate_item(x)


def _validate_test_data(test_data):
  """Validates test_data.

  This function checks that test_data is a non-empty tuple of valid rows.

  Args:
    test_data: A tuple with rows.
  """
  if not test_data:
    raise ValueError('No rows')
  for row in test_data:
    _validate_row(row)


def _denorm_value(x):
  """Returns a denormalized value with a desirable __eq__() behaviour.

  For example:
                 -0.0  ==              0.0 : True
    denorm_value(-0.0) == denorm_value(0.0): False

                  NaN  ==              nan : False
    denorm_value( NaN) == denorm_value(NaN): True

  Args:
    x: A scalar value.

  Returns:
    Denormalization of the value.
  """
  if isinstance(x, arolla.QValue):
    return (x.qtype, x.fingerprint)
  if isinstance(x, float):
    return (float, struct.pack('f', x))
  return x


@functools.lru_cache()
def _group_by_columns(test_data, *column_indices):
  """Returns blocks of the test_data with fixed values in the specified columns.

  This functions groups rows of test_data into blocks by values in the specific
  columns.

  Example:

    test_data = (
      ('a', 5, 10),
      ('b', 7, 10),
      ('a', 7, 10)
    )

    1. _group_by_column(test_data) = (
         (
           ('a', 5, 10),
           ('a', 7, 10)
           ('b', 7, 10)
         )
       )

    2. _group_by_column(test_data, 0) = (
         (
           ('a', 5, 10),
           ('a', 7, 10)
         ),
         (
           ('b', 7, 10)
         )
       )

    3. _group_by_column(test_data, 1, 2) = (
         (
           ('a', 5, 10)
         ),
         (
           ('b', 7, 10),
           ('a', 7, 10)
         )
       )

  Args:
    test_data: A tuple with rows.
    *column_indices: Indices of columns for groupping.
  """

  def key_fn(row):
    return tuple(_denorm_value(row[i]) for i in column_indices)

  groups = {}
  for row in test_data:
    key = key_fn(row)
    group = groups.get(key)
    if group is None:
      groups[key] = [row]
    else:
      group.append(row)
  return tuple(tuple(group) for group in groups.values())


# Predicates for item filtering per specific qtype


def _is_qtype(value):
  return isinstance(value, arolla.QValue) and value.qtype == arolla.QTYPE


def _is_unit(value):
  # pylint: disable=g-bool-id-comparison
  return value is True or (
      isinstance(value, arolla.QValue) and value.qtype == arolla.UNIT
  )


def _is_boolean(value):
  return isinstance(value, bool) or (
      isinstance(value, arolla.QValue) and value.qtype == arolla.BOOLEAN
  )


def _is_bytes(value):
  return isinstance(value, bytes) or (
      isinstance(value, arolla.QValue) and value.qtype == arolla.BYTES
  )


def _is_text(value):
  return isinstance(value, str) or (
      isinstance(value, arolla.QValue) and value.qtype == arolla.TEXT
  )


def _is_int32(value):
  return (
      isinstance(value, int)
      and not isinstance(value, bool)
      and (-(2**31)) <= value < 2**31
  ) or (isinstance(value, arolla.QValue) and value.qtype == arolla.INT32)


def _is_int64(value):
  return (
      isinstance(value, int)
      and not isinstance(value, bool)
      and (-(2**63)) <= value < 2**63
  ) or (isinstance(value, arolla.QValue) and value.qtype == arolla.INT64)


def _is_uint64(value):
  return (
      isinstance(value, int)
      and not isinstance(value, bool)
      and 0 <= value < 2**64
  ) or (isinstance(value, arolla.QValue) and value.qtype == arolla.types.UINT64)


def _is_float32(value):
  """Returns True if the value is compatible with float32."""
  if isinstance(value, float):
    return math.isfinite(arolla.float32(value).py_value()) == math.isfinite(
        value
    )
  if isinstance(value, int):
    if isinstance(value, (bool, IntOnly)):
      return False
    try:
      return arolla.float32(float(value)).py_value() == value
    except OverflowError:
      pass
  if isinstance(value, arolla.QValue):
    return value.qtype == arolla.FLOAT32
  return False


def _is_float64(value):
  """Returns True if the value is compatible with float64."""
  if isinstance(value, float):
    return True
  if isinstance(value, int):
    if isinstance(value, (bool, IntOnly)):
      return False
    try:
      return float(value) == value
    except OverflowError:
      pass
  if isinstance(value, arolla.QValue):
    return value.qtype == arolla.FLOAT64
  return False


def _is_weak_float(value):
  """Returns True if the value is compatible with float64."""
  if isinstance(value, float):
    return True
  if isinstance(value, int):
    if isinstance(value, (bool, IntOnly)):
      return False
    try:
      return float(value) == value
    except OverflowError:
      pass
  if isinstance(value, arolla.QValue):
    return value.qtype == arolla.WEAK_FLOAT
  return False


_item_filter_predicates = {
    None: lambda x: True,
    arolla.QTYPE: _is_qtype,
    arolla.UNIT: _is_unit,
    arolla.BOOLEAN: _is_boolean,
    arolla.BYTES: _is_bytes,
    arolla.TEXT: _is_text,
    arolla.INT32: _is_int32,
    arolla.INT64: _is_int64,
    arolla.types.UINT64: _is_uint64,
    arolla.FLOAT32: _is_float32,
    arolla.FLOAT64: _is_float64,
    arolla.WEAK_FLOAT: _is_weak_float,
    arolla.OPTIONAL_UNIT: lambda x: x is None or _is_unit(x),
    arolla.OPTIONAL_BOOLEAN: lambda x: x is None or _is_boolean(x),
    arolla.OPTIONAL_BYTES: lambda x: x is None or _is_bytes(x),
    arolla.OPTIONAL_TEXT: lambda x: x is None or _is_text(x),
    arolla.OPTIONAL_INT32: lambda x: x is None or _is_int32(x),
    arolla.OPTIONAL_INT64: lambda x: x is None or _is_int64(x),
    arolla.types.OPTIONAL_UINT64: lambda x: x is None or _is_uint64(x),
    arolla.OPTIONAL_FLOAT32: lambda x: x is None or _is_float32(x),
    arolla.OPTIONAL_FLOAT64: lambda x: x is None or _is_float64(x),
    arolla.OPTIONAL_WEAK_FLOAT: lambda x: x is None or _is_weak_float(x),
    arolla.DENSE_ARRAY_UNIT: lambda x: x is None or _is_unit(x),
    arolla.DENSE_ARRAY_BOOLEAN: lambda x: x is None or _is_boolean(x),
    arolla.DENSE_ARRAY_BYTES: lambda x: x is None or _is_bytes(x),
    arolla.DENSE_ARRAY_TEXT: lambda x: x is None or _is_text(x),
    arolla.DENSE_ARRAY_INT32: lambda x: x is None or _is_int32(x),
    arolla.DENSE_ARRAY_INT64: lambda x: x is None or _is_int64(x),
    arolla.types.DENSE_ARRAY_UINT64: lambda x: x is None or _is_uint64(x),
    arolla.DENSE_ARRAY_FLOAT32: lambda x: x is None or _is_float32(x),
    arolla.DENSE_ARRAY_FLOAT64: lambda x: x is None or _is_float64(x),
    arolla.DENSE_ARRAY_WEAK_FLOAT: lambda x: x is None or _is_weak_float(x),
    arolla.ARRAY_UNIT: lambda x: x is None or _is_unit(x),
    arolla.ARRAY_BOOLEAN: lambda x: x is None or _is_boolean(x),
    arolla.ARRAY_BYTES: lambda x: x is None or _is_bytes(x),
    arolla.ARRAY_TEXT: lambda x: x is None or _is_text(x),
    arolla.ARRAY_INT32: lambda x: x is None or _is_int32(x),
    arolla.ARRAY_INT64: lambda x: x is None or _is_int64(x),
    arolla.types.ARRAY_UINT64: lambda x: x is None or _is_uint64(x),
    arolla.ARRAY_FLOAT32: lambda x: x is None or _is_float32(x),
    arolla.ARRAY_FLOAT64: lambda x: x is None or _is_float64(x),
    arolla.ARRAY_WEAK_FLOAT: lambda x: x is None or _is_weak_float(x),
}

# Factories for qvalue construction from a column data.
#
# The factories expect the data to be pre-grouped (all items in a column for
# a scalar/optional type are the same) and pre-filtered (only the compatible
# values have remained).

_column_to_qvalue_factories = {
    None: lambda x: x[0],
    arolla.QTYPE: lambda x: x[0],
    arolla.UNIT: lambda _: arolla.unit(),
    arolla.BOOLEAN: lambda x: arolla.boolean(x[0]),
    arolla.BYTES: lambda x: arolla.bytes(x[0]),
    arolla.TEXT: lambda x: arolla.text(x[0]),
    arolla.INT32: lambda x: arolla.int32(x[0]),
    arolla.INT64: lambda x: arolla.int64(x[0]),
    arolla.types.UINT64: lambda x: arolla.types.uint64(x[0]),
    arolla.FLOAT32: lambda x: arolla.float32(x[0]),
    arolla.FLOAT64: lambda x: arolla.float64(x[0]),
    arolla.WEAK_FLOAT: lambda x: arolla.weak_float(x[0]),
    arolla.OPTIONAL_UNIT: lambda x: arolla.optional_unit(x[0]),
    arolla.OPTIONAL_BOOLEAN: lambda x: arolla.optional_boolean(x[0]),
    arolla.OPTIONAL_BYTES: lambda x: arolla.optional_bytes(x[0]),
    arolla.OPTIONAL_TEXT: lambda x: arolla.optional_text(x[0]),
    arolla.OPTIONAL_INT32: lambda x: arolla.optional_int32(x[0]),
    arolla.OPTIONAL_INT64: lambda x: arolla.optional_int64(x[0]),
    arolla.types.OPTIONAL_UINT64: lambda x: arolla.types.optional_uint64(x[0]),
    arolla.OPTIONAL_FLOAT32: lambda x: arolla.optional_float32(x[0]),
    arolla.OPTIONAL_FLOAT64: lambda x: arolla.optional_float64(x[0]),
    arolla.OPTIONAL_WEAK_FLOAT: lambda x: arolla.optional_weak_float(x[0]),
    arolla.DENSE_ARRAY_UNIT: arolla.dense_array_unit,
    arolla.DENSE_ARRAY_BOOLEAN: arolla.dense_array_boolean,
    arolla.DENSE_ARRAY_BYTES: arolla.dense_array_bytes,
    arolla.DENSE_ARRAY_TEXT: arolla.dense_array_text,
    arolla.DENSE_ARRAY_INT32: arolla.dense_array_int32,
    arolla.DENSE_ARRAY_INT64: arolla.dense_array_int64,
    arolla.types.DENSE_ARRAY_UINT64: arolla.types.dense_array_uint64,
    arolla.DENSE_ARRAY_FLOAT32: arolla.dense_array_float32,
    arolla.DENSE_ARRAY_FLOAT64: arolla.dense_array_float64,
    arolla.DENSE_ARRAY_WEAK_FLOAT: arolla.dense_array_weak_float,
    arolla.ARRAY_UNIT: arolla.array_unit,
    arolla.ARRAY_BOOLEAN: arolla.array_boolean,
    arolla.ARRAY_BYTES: arolla.array_bytes,
    arolla.ARRAY_TEXT: arolla.array_text,
    arolla.ARRAY_INT32: arolla.array_int32,
    arolla.ARRAY_INT64: arolla.array_int64,
    arolla.types.ARRAY_UINT64: arolla.types.array_uint64,
    arolla.ARRAY_FLOAT32: arolla.array_float32,
    arolla.ARRAY_FLOAT64: arolla.array_float64,
    arolla.ARRAY_WEAK_FLOAT: arolla.array_weak_float,
}


def matching_qtypes(value, candidate_qtypes):
  """Yields all the candidate qtypes that can represent value."""
  match_found = False
  for t in candidate_qtypes:
    if _item_filter_predicates[t](value):
      match_found = True
      yield t
  if not match_found:
    raise RuntimeError(f'No matching qtypes found for value `{value}`')


def _gen_cases_impl(test_data, *qtypes):
  """Yields test cases with the given qtypes from the test_data.

  Depends on the qtypes, this function groups the test_data rows, filters
  compatible items, and yields tuples with qvalues.

  Example:
    test_data = (
      ('a', 5, 10),
      ('b', 7, 10),
      ('a', 7, 10)
    )

    1. _gen_cases_impl(
         test_data,
         arolla.ARRAY_TEXT,
         arolla.ARRAY_INT32,
         arolla.ARRAY_INT32
       ) = (
         (
           array(['a', 'b', 'a']),
           array([5, 7, 7]),
           array([10, 10, 10])
         )
       )

    2. _gen_cases_impl(test_data, arolla.TEXT, arolla.ARRAY_INT32,
    arolla.ARRAY_INT32) = (
         ('a', array([5, 7]), array([10, 10])),
         ('b', array([7]), array([10]))
       )

    3. _gen_cases_impl(test_data, arolla.ARRAY_TEXT, arolla.INT32, arolla.INT32)
    = (
         (array(['a']), 5, 10),
         (array(['a', 'b']), 7, 10)
       )

  Args:
    test_data: A tuple with rows.
    *qtypes: QTypes of qvalues to produce. None indicates that the corresponding
      column items should not be converted.

  Yields:
    Tuples with qvalues of the specified qtypes.

  Raises:
    RuntimeError: No test data for the given qtypes.
  """
  array_like_qtypes = set(
      arolla.types.ARRAY_QTYPES
      + arolla.types.DENSE_ARRAY_QTYPES
      + arolla.types.ARRAY_QTYPES
  )
  row_groups = _group_by_columns(
      test_data,
      *(i for i, qtype in enumerate(qtypes) if qtype not in array_like_qtypes),
  )
  item_filter_predicates = tuple(
      _item_filter_predicates[qtype] for qtype in qtypes
  )
  column_to_qvalue_factories = tuple(
      _column_to_qvalue_factories[qtype] for qtype in qtypes
  )

  empty = True
  for row_group in row_groups:
    row_group = tuple(
        row
        for row in row_group
        if all(pred(value) for pred, value in zip(item_filter_predicates, row))
    )

    if not row_group:  # skip empty groups
      continue

    yield tuple(
        factory(column)
        for factory, column in zip(column_to_qvalue_factories, zip(*row_group))
    )

    empty = False
  if empty:
    raise RuntimeError(
        'No test data for a qtype case: ' + ', '.join(map(str, qtypes))
    )


def gen_cases(test_data, *qtype_sets):
  """Yields test cases for each of the given qtype combinations.

  Example:

    test_data = (
      ('a', 5, 10),
      ('b', 7, 10),
      ('a', 7, 10),
      ('a', 7, 10, 57),
    )

    gen_cases(
         test_data,
         (arolla.ARRAY_TEXT, arolla.ARRAY_INT32, arolla.ARRAY_INT32),
         (arolla.TEXT, arolla.ARRAY_INT32, arolla.ARRAY_INT32),
         (arolla.TEXT, arolla.ARRAY_INT32, arolla.ARRAY_INT32),
         (arolla.ARRAY_TEXT, arolla.INT32, arolla.INT32),
         (arolla.ARRAY_TEXT, arolla.INT32, arolla.INT32, arolla.INT32)
       ) = (
         (array(['a', 'b', 'a']), array([5, 7, 7]), array([10, 10, 10])),
         ('a', array([5, 7]), array([10, 10])),
         ('b', array([7]), array([10])),
         (array(['a']), 5, 10),
         (array(['a', 'b']), 7, 10),
         (array(['a']), 7, 10, 57),
       )

  Args:
    test_data: A tuple with rows of test data. Lengths of the rows correspond to
      arity of the tested operator.
    *qtype_sets: Tuples of qtypes to generate cases for. `None` indicates that
      the corresponding value doesn't need to be converted to qvalue. Each tuple
      is matched pointwise against each row of the same length in test_data.

  Yields:
    Tuples with qvalues corresponding to the given combinations of the qtypes.

  The logic of picking value combinations for each qtype_set is the following:
    - If the qtype_set does not contain array types, the resulting tuples are
      constructed row-wise.
    - If qtype_set contains only arrays, several rows of the matching scalar
      types will be transposed into a row of arrays.
    - If qtype_set contains both arrays and non-arrays, the frunction will group
      rows with equal values in non-array columns, combining values from array
      columns into arrays.
    - The function raises an error if it does not find any matching value
      combination for a qtype set.
  It is guaranteed that all the arrays constructed by the function have the same
  size and correspond row-wise to test_data rows.
  """
  if not isinstance(test_data, tuple):
    raise ValueError(
        'Expected a tuple with rows, got {}'.format(
            arolla.abc.get_type_name(type(test_data))
        )
    )

  _validate_test_data(test_data)
  test_data_by_len = {
      k: tuple(g)
      for k, g in itertools.groupby(sorted(test_data, key=len), key=len)
  }
  for qtypes in qtype_sets:
    yield from _gen_cases_impl(test_data_by_len[len(qtypes)], *qtypes)


def lift_qtypes(*args, mutators=ALL_QTYPE_MUTATORS):
  """Returns combinations of qtypes after applying lifting.

  This function keeps None values as-is.

  Args:
    *args: Optional[QType] (or a tuple of Optional[QType]).
    mutators: Mutators for qtypes.

  Returns:
    Mutated qtypes (or a combinations of mutated QTypes).
  """
  result = {}  # ordered
  for arg in args:
    if isinstance(arg, tuple) and all(
        x is None or isinstance(x, arolla.QType) for x in arg
    ):
      for mutator in mutators:
        result[tuple(None if x is None else mutator(x) for x in arg)] = None
    elif isinstance(arg, arolla.QType):
      for mutator in mutators:
        result[mutator(arg)] = None
    elif arg is None:
      if mutators:
        result[None] = None
    else:
      raise TypeError(f'expected a qtype or a tuple of qtypes, got {arg!r}')
  return tuple(result.keys())


detect_qtype_signatures = arolla.testing.detect_qtype_signatures
DETECT_SIGNATURES_DEFAULT_QTYPES = (
    arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES
)
