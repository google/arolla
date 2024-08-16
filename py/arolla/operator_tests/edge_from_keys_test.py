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

"""Tests for edge.from_keys operator."""

import dataclasses
import itertools
from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


_VALUE_QTYPES = [
    arolla.BYTES,
    arolla.INT32,
    arolla.INT64,
    arolla.FLOAT32,
    arolla.FLOAT64,
]

_ARRAY_QTYPES = [arolla.make_array_qtype(qtype) for qtype in _VALUE_QTYPES]
_ARRAY_QTYPES += [
    arolla.make_dense_array_qtype(qtype) for qtype in _VALUE_QTYPES
]

_TUPLE_OF_ONE_QTYPES = list(
    arolla.types.make_tuple_qtype(x) for x in _ARRAY_QTYPES
)
_TUPLE_OF_TWO_QTYPES = list(
    arolla.types.make_tuple_qtype(x, y)
    for x, y in itertools.product(_ARRAY_QTYPES, _ARRAY_QTYPES)
)

# All qtypes that we consider existing in the qtype signatures test
_ALL_POSSIBLE_QTYPES = (
    list(pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES)
    + _TUPLE_OF_ONE_QTYPES
    + _TUPLE_OF_TWO_QTYPES
)


@dataclasses.dataclass
class EdgeFromKeysTestParameters:
  child_keys: list[Any]
  parent_keys: list[Any]
  mapping: list[int | None]
  child_qtype: arolla.QType
  parent_qtype: arolla.QType


def get_test_data():
  """Returns test cases for edge.from_keys operator."""
  integral_data = list()
  for child_qtype in arolla.types.INTEGRAL_QTYPES:
    for parent_qtype in arolla.types.INTEGRAL_QTYPES:
      if (
          arolla.eval(M.qtype.common_qtype(child_qtype, parent_qtype))
          == parent_qtype
      ):
        integral_data.append(
            EdgeFromKeysTestParameters(
                child_keys=[-1, None, 2, 1, 3, 2, None, -1, 6],
                parent_keys=[-1, None, 2, 6, 3],
                mapping=[0, None, 2, None, 4, 2, None, 0, 3],
                child_qtype=child_qtype,
                parent_qtype=parent_qtype,
            )
        )
  boolean_data = [
      EdgeFromKeysTestParameters(
          child_keys=[True, None, True, False],
          parent_keys=[False, None, True],
          mapping=[2, None, 2, 0],
          child_qtype=arolla.types.BOOLEAN,
          parent_qtype=arolla.types.BOOLEAN,
      )
  ]
  bytes_data = [
      EdgeFromKeysTestParameters(
          child_keys=[b'a', b'aba', None, b'b', b'a'],
          parent_keys=[b'a', b'aba', None],
          mapping=[0, 1, None, None, 0],
          child_qtype=arolla.types.BYTES,
          parent_qtype=arolla.types.BYTES,
      )
  ]
  text_data = [
      EdgeFromKeysTestParameters(
          child_keys=['a', 'aba', None, 'b', 'a'],
          parent_keys=['a', 'aba', None],
          mapping=[0, 1, None, None, 0],
          child_qtype=arolla.types.TEXT,
          parent_qtype=arolla.types.TEXT,
      )
  ]
  uint64_data = [
      EdgeFromKeysTestParameters(
          child_keys=[None, 2, 1, 1, None],
          parent_keys=[0, 2, None],
          mapping=[None, 1, None, None, None],
          child_qtype=arolla.types.UINT64,
          parent_qtype=arolla.types.UINT64,
      )
  ]
  test_data = itertools.chain(
      integral_data, boolean_data, bytes_data, text_data, uint64_data
  )
  return test_data


def gen_test_cases(test_data):
  for example in test_data:
    for _, array_fn in utils.ARRAY_FACTORIES:
      yield (
          array_fn(example.child_keys, value_qtype=example.child_qtype),
          array_fn(example.parent_keys, value_qtype=example.parent_qtype),
          array_fn(example.mapping, value_qtype=arolla.INT64),
          len(example.parent_keys),
      )


def gen_test_cases_tuples(test_data):
  for example in test_data:
    for _, array_fn in utils.ARRAY_FACTORIES:
      # Here we generate some trivial test cases to check that edge.from_keys
      # works with tuples of different qtypes. Some non-trivial test cases are
      # defined separately.
      child_keys = M.core.make_tuple(
          array_fn(example.child_keys, value_qtype=example.child_qtype),
          array_fn(example.child_keys, value_qtype=example.child_qtype),
          array_fn([0] * len(example.child_keys), value_qtype=arolla.INT64),
      )
      parent_keys = M.core.make_tuple(
          array_fn(example.parent_keys, value_qtype=example.parent_qtype),
          array_fn(example.parent_keys, value_qtype=example.parent_qtype),
          array_fn([0] * len(example.parent_keys), value_qtype=arolla.INT64),
      )
      yield (
          child_keys,
          parent_keys,
          array_fn(example.mapping, value_qtype=arolla.INT64),
          len(example.parent_keys),
      )


def get_qtype_signature(child_keys, parent_keys):
  return (
      child_keys.qtype,
      parent_keys.qtype,
      arolla.DENSE_ARRAY_EDGE
      if arolla.eval(M.qtype.is_dense_array_qtype(child_keys.qtype))
      else arolla.ARRAY_EDGE,
  )


def get_tuple_signatures(value_qtypes):
  value_qtype_pairs = []
  for child_qtype in value_qtypes:
    if child_qtype in arolla.types.FLOATING_POINT_QTYPES:
      continue
    for parent_qtype in value_qtypes:
      if parent_qtype in arolla.types.FLOATING_POINT_QTYPES:
        continue
      if (
          arolla.eval(M.qtype.common_qtype(child_qtype, parent_qtype))
          == parent_qtype
      ):
        value_qtype_pairs.append((child_qtype, parent_qtype))

  for array_qtype_fn, edge_qtype in (
      (arolla.make_array_qtype, arolla.ARRAY_EDGE),
      (arolla.make_dense_array_qtype, arolla.DENSE_ARRAY_EDGE),
  ):
    for child_qtype, parent_qtype in value_qtype_pairs:
      yield (
          arolla.make_tuple_qtype(array_qtype_fn(child_qtype)),
          arolla.make_tuple_qtype(array_qtype_fn(parent_qtype)),
          edge_qtype,
      )
      for child_qtype_b, parent_qtype_b in value_qtype_pairs:
        child_keys_qtype = arolla.make_tuple_qtype(
            array_qtype_fn(child_qtype), array_qtype_fn(child_qtype_b)
        )
        parent_keys_qtype = arolla.make_tuple_qtype(
            array_qtype_fn(parent_qtype), array_qtype_fn(parent_qtype_b)
        )
        yield (child_keys_qtype, parent_keys_qtype, edge_qtype)


TEST_CASES = tuple(gen_test_cases(get_test_data()))
TEST_CASES_TUPLES = tuple(gen_test_cases_tuples(get_test_data()))


# QType signatures for edge.from_keys.
#
#   ((child_keys_qtype, parent_keys_qtype, result_qtype), ...)
QTYPE_SIGNATURES = [
    get_qtype_signature(child_keys, parent_keys)
    for child_keys, parent_keys, _, _ in TEST_CASES
] + list(get_tuple_signatures(_VALUE_QTYPES))


class EdgeFromKeysTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.edge.from_keys, QTYPE_SIGNATURES, possible_qtypes=_ALL_POSSIBLE_QTYPES
    )

  @parameterized.parameters(*(TEST_CASES + TEST_CASES_TUPLES))
  def test_simple(self, child_keys, parent_keys, mapping, mapping_size):
    result = self.eval(M.edge.from_keys(child_keys, parent_keys))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)), mapping
    )
    self.assertEqual(result.parent_size, mapping_size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_with_missing_child_keys(self, array_factory):
    child_keys = array_factory([1, None, 3, 4])
    parent_keys = array_factory([4, 1, 3, 2])
    result = self.eval(M.edge.from_keys(child_keys, parent_keys))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)),
        array_factory([1, None, 2, 0], arolla.INT64),
    )
    self.assertEqual(result.parent_size, parent_keys.size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_with_missing_parent_keys(self, array_factory):
    child_keys = array_factory([1, 2, 3, 4])
    parent_keys = array_factory([4, 1, None, 2])
    result = self.eval(M.edge.from_keys(child_keys, parent_keys))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)),
        array_factory([1, 3, None, 0], arolla.INT64),
    )
    self.assertEqual(result.parent_size, parent_keys.size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_with_extra_value(self, array_factory):
    child_keys = array_factory([5, 7, 1, 2, 4, 8])
    parent_keys = array_factory([1, 2, 3, 4, 5, 6, 7])
    result = self.eval(M.edge.from_keys(child_keys, parent_keys))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)),
        array_factory([4, 6, 0, 1, 3, None], arolla.INT64),
    )
    self.assertEqual(result.parent_size, parent_keys.size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_duplicates_in_child_keys(self, array_factory):
    child_keys = array_factory(['5', '7', '5', '2', '4', '8'])
    parent_keys = array_factory(['1', '2', '3', '4', '5', '6', '7'])
    result = self.eval(M.edge.from_keys(child_keys, parent_keys))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)),
        array_factory([4, 6, 4, 1, 3, None], arolla.INT64),
    )
    self.assertEqual(result.parent_size, parent_keys.size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_duplicates_in_parent_keys(self, array_factory):
    child_keys = array_factory([5, 7, 5, 2, 4, 8])
    parent_keys = array_factory([1, 2, 3, 3])
    with self.assertRaisesRegex(ValueError, 'duplicated key'):
      _ = self.eval(M.edge.from_keys(child_keys, parent_keys))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_tuples(self, array_factory):
    child_keys = M.core.make_tuple(
        array_factory([1, 2, 3, 2, 3, 4], value_qtype=arolla.INT32),
        array_factory([1, 1, 1, 2, 2, 2], value_qtype=arolla.INT32),
    )
    parent_keys = M.core.make_tuple(
        array_factory([1, 2, 3, 4, 1, 2, 3], value_qtype=arolla.INT64),
        array_factory([1, 1, 1, 1, 2, 2, 2], value_qtype=arolla.INT64),
    )
    result = self.eval(M.edge.from_keys(child_keys, parent_keys))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)),
        array_factory([0, 1, 2, 5, 6, None], arolla.INT64),
    )
    self.assertEqual(
        result.parent_size, arolla.eval(M.core.get_first(parent_keys)).size
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_different_sizes_in_child_keys(self, array_factory):
    child_keys = M.core.make_tuple(
        array_factory([1, 2], value_qtype=arolla.INT32),
        array_factory([1], value_qtype=arolla.INT32),
    )
    parent_keys = M.core.make_tuple(
        array_factory([3, 4], value_qtype=arolla.INT32),
        array_factory([2, 3], value_qtype=arolla.INT32),
    )
    with self.assertRaisesRegex(
        ValueError,
        'expected all arrays in child_keys tuple to have the same size',
    ):
      _ = self.eval(M.edge.from_keys(child_keys, parent_keys))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_different_sizes_in_parent_keys(self, array_factory):
    child_keys = M.core.make_tuple(
        array_factory([1, 2], value_qtype=arolla.INT32),
        array_factory([1, 1], value_qtype=arolla.INT32),
    )
    parent_keys = M.core.make_tuple(
        array_factory([2, 3, 4], value_qtype=arolla.INT32),
        array_factory([2, 3], value_qtype=arolla.INT32),
    )
    with self.assertRaisesRegex(
        ValueError,
        'expected all arrays in parent_keys tuple to have the same size',
    ):
      _ = self.eval(M.edge.from_keys(child_keys, parent_keys))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_different_sizes(self, array_factory):
    child_keys = M.core.make_tuple(
        array_factory([1, 2], value_qtype=arolla.INT32),
        array_factory([1], value_qtype=arolla.INT32),
    )
    parent_keys = M.core.make_tuple(
        array_factory([3, 4], value_qtype=arolla.INT32),
        array_factory([2, 3, 4], value_qtype=arolla.INT32),
    )
    with self.assertRaisesRegex(
        ValueError, 'expected all arrays in .*keys tuple to have the same size'
    ):
      _ = self.eval(M.edge.from_keys(child_keys, parent_keys))


if __name__ == '__main__':
  absltest.main()
