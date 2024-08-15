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

"""Tests for M.array.collapse."""

import itertools
import random

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M

nan = float('nan')

# Test data for arary.collapse operator.
#
# (
#   (values, agg_result),
#   ...
# )
#

INTEGER_TEST_DATA = (
    ([], None),
    ([None], None),
    ([None, None], None),
    ([1, None], 1),
    ([None, 1], 1),
    ([1, None, 1], 1),
    ([2, 3], None),
)

FLOATING_POINT_TEST_DATA = (
    ([], None),
    ([None], None),
    ([None, None], None),
    ([1, None], 1),
    ([None, 1], 1),
    ([1, None, 1], 1),
    ([2, 3], None),
    # special cases
    ([-0.0, +0.0], 0.0),
    ([nan], nan),
    ([nan, nan], nan),
    ([nan, None, nan], nan),
    ([nan, 0.0, nan], None),
)

UNIT_TEST_DATA = (
    ([], None),
    ([None], None),
    ([None, None], None),
    ([True, None], True),
    ([None, True], True),
    ([True, None, True], True),
)

BOOLEAN_TEST_DATA = (
    ([], None),
    ([None], None),
    ([None, None], None),
    ([True, None], True),
    ([None, True], True),
    ([True, None, True], True),
    ([True, False], None),
)

TEXT_TEST_DATA = (
    ([], None),
    ([None], None),
    ([None, None], None),
    (['foo', None], 'foo'),
    ([None, 'foo'], 'foo'),
    (['foo', None, 'foo'], 'foo'),
    (['bar', 'baz'], None),
)

BYTES_TEST_DATA = (
    ([], None),
    ([None], None),
    ([None, None], None),
    ([b'foo', None], b'foo'),
    ([None, b'foo'], b'foo'),
    ([b'foo', None, b'foo'], b'foo'),
    ([b'bar', b'baz'], None),
)


def gen_cases_impl(test_data, array_fn, scalar_qtype):
  """Yields test cases."""
  as_array = lambda x: array_fn(x, scalar_qtype)
  as_optional = lambda x: utils.optional(x, scalar_qtype)
  for arg, result in test_data:
    # (array, result)
    yield 'no_edge', as_array(arg), as_array([result])
    yield 'unspecified_edge', as_array(arg), arolla.unspecified(), as_array(
        [result]
    )
    # (array, edge_to_single, result)
    yield (
        'edge_to_single',
        as_array(arg),
        arolla.eval(M.edge.to_single(as_array(arg))),
        as_array([result]),
    )
    # (array, edge_to_scalar, result)
    yield (
        'edge_to_scalar',
        as_array(arg),
        arolla.eval(M.edge.to_scalar(as_array(arg))),
        as_optional(result),
    )
  # (array, edge_split_points, result)
  arg = []
  sizes = []
  result = []
  for group_arg, group_result in test_data:
    arg.extend(group_arg)
    sizes.append(len(group_arg))
    result.append(group_result)
  yield (
      'edge_split_points',
      as_array(arg),
      arolla.eval(M.edge.from_sizes(array_fn(sizes, arolla.INT64))),
      as_array(result),
  )
  # (empty_array, edge_split_points, result)
  yield (
      'empty_array_with_edge',
      as_array([]),
      arolla.eval(M.edge.from_sizes(array_fn([0, 0, 0], arolla.INT64))),
      as_array([None, None, None]),
  )
  # (array, edge_mapping, result)
  mapping = list(
      itertools.chain.from_iterable([i] * size for i, size in enumerate(sizes))
  )
  seed = 4
  random.Random(seed).shuffle(arg)
  random.Random(seed).shuffle(mapping)
  yield (
      'edge_mapping',
      as_array(arg),
      arolla.eval(
          M.edge.from_mapping(array_fn(mapping, arolla.INT64), len(sizes))
      ),
      as_array(result),
  )


def gen_cases():
  """Yields test cases for array.collapse operator.

  Yields: (test_tag, *arg_qvalues, result_qvalue), where test_tag indicates the
    type of edge.
  """
  for array_fn in (arolla.array, arolla.dense_array):
    for scalar_qtype in arolla.types.INTEGRAL_QTYPES + (arolla.types.UINT64,):
      yield from gen_cases_impl(INTEGER_TEST_DATA, array_fn, scalar_qtype)
    for scalar_qtype in arolla.types.FLOATING_POINT_QTYPES:
      yield from gen_cases_impl(
          FLOATING_POINT_TEST_DATA, array_fn, scalar_qtype
      )
    yield from gen_cases_impl(UNIT_TEST_DATA, array_fn, arolla.UNIT)
    yield from gen_cases_impl(BOOLEAN_TEST_DATA, array_fn, arolla.BOOLEAN)
    yield from gen_cases_impl(BYTES_TEST_DATA, array_fn, arolla.BYTES)
    yield from gen_cases_impl(TEXT_TEST_DATA, array_fn, arolla.TEXT)


TEST_CASES = tuple(gen_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case[1:]) for test_case in TEST_CASES
)


class ArrayCollapseTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.array.collapse)
        ),
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, unused_tag, *qvalues):
    input_qvalues = qvalues[:-1]
    expected_qvalue = qvalues[-1]
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.array.collapse(*input_qvalues)),
        expected_qvalue,
        # tolerate difference between -0.0, 0.0
        rtol=0.0,
        atol=0.0,
    )

  def test_regression_nan_after_value(self):
    actual_qvalue = self.eval(M.array.collapse([2.0, nan]))
    arolla.testing.assert_qvalue_allequal(
        actual_qvalue, arolla.array_float32([None])
    )

  def test_regression_empty_group_reset(self):
    edge = arolla.types.ArrayEdge.from_sizes([1, 0])
    actual_qvalue = self.eval(M.array.collapse([1.0], edge))
    arolla.testing.assert_qvalue_allequal(
        actual_qvalue, arolla.array([1.0, None])
    )


if __name__ == '__main__':
  absltest.main()
