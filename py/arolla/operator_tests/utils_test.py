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

"""Tests for utils.py."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M2 = arolla.M
P2 = arolla.P
constraints = arolla.optools.constraints


class TestSampleArrays(parameterized.TestCase):

  def test_size_0(self):
    arrays = list(utils.sample_arrays(arolla.array([None, 0]), size=0))
    self.assertLen(arrays, 1)
    arolla.testing.assert_qvalue_allequal(arrays[0], arolla.array_int32([]))

  def test_array_size_5(self):
    arrays = list(utils.sample_arrays(arolla.array([None, 0, 1]), size=5))
    self.assertLen(arrays, 3**5)
    for x in arrays:
      self.assertEqual(x.qtype, arolla.ARRAY_INT32)
      self.assertTrue(set(x.py_value()).issubset({0, 1, None}))

  def test_dense_array(self):
    arrays = list(
        utils.sample_arrays(arolla.dense_array([None, 0.0, 1.5]), size=5)
    )
    self.assertLen(arrays, 3**5)
    for x in arrays:
      self.assertEqual(x.qtype, arolla.DENSE_ARRAY_FLOAT32)
      self.assertTrue(set(x.py_value()).issubset({0.0, 1.5, None}))

  def test_error_huge_count(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected result size is 1024; please specify allow_huge_count=True',
    ):
      _ = list(utils.sample_arrays(arolla.array([0, 1]), size=10))

  def test_allow_huge_count(self):
    arrays = list(
        utils.sample_arrays(
            arolla.array([0, 1]), size=10, allow_huge_count=True
        )
    )
    self.assertLen(arrays, 1024)
    for x in arrays:
      self.assertEqual(x.qtype, arolla.ARRAY_INT32)
      self.assertTrue(set(x.py_value()).issubset({0, 1}))

  def test_limit_count(self):
    arrays = list(
        utils.sample_arrays(arolla.array([0, 1]), size=10, count=1000)
    )
    self.assertLen(arrays, 1000)
    for x in arrays:
      self.assertEqual(x.qtype, arolla.ARRAY_INT32)
      self.assertTrue(set(x.py_value()).issubset({0, 1}))

  def test_empty_result(self):
    arrays = list(utils.sample_arrays(arolla.array_int32([]), size=10))
    self.assertEmpty(arrays)

  def test_count_auto_limit(self):
    arrays = list(utils.sample_arrays(arolla.array([0]), size=10, count=100))
    self.assertLen(arrays, 1)
    self.assertEqual(arrays[0].qtype, arolla.ARRAY_INT32)
    self.assertEqual(set(arrays[0].py_value()), {0})

  def test_seed(self):
    base_array = arolla.array([0, 1])
    r1 = [
        x.fingerprint
        for x in utils.sample_arrays(base_array, size=5, count=4, seed=1)
    ]
    r2 = [
        x.fingerprint
        for x in utils.sample_arrays(base_array, size=5, count=4, seed=1)
    ]
    r3 = [
        x.fingerprint
        for x in utils.sample_arrays(base_array, size=5, count=4, seed=2)
    ]
    self.assertEqual(r1, r2)
    self.assertNotEqual(r1, r3)

  def test_error_not_array(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'unsupported array type: OPTIONAL_UNIT'
    ):
      _ = list(utils.sample_arrays(arolla.optional_unit(None), size=10))

  def test_uniqueness(self):
    samples = set(
        x.fingerprint
        for x in utils.sample_arrays(
            arolla.array([0, 1]), 10, count=1023, allow_huge_count=True
        )
    )
    self.assertLen(samples, 1023)


class TestGenSimpleAggIntoCases(parameterized.TestCase):

  def test_smoke_size_op(self):

    @arolla.optools.as_lambda_operator(
        'size_op',
        qtype_constraints=[
            constraints.expect_array(P2.x),
            *constraints.expect_edge_or_unspecified(
                P2.into, child_side_param=P2.x
            ),
        ],
    )
    def size_op(x, into=arolla.unspecified()):
      return M2.edge.sizes(
          M2.core.default_if_unspecified(into, M2.edge.to_scalar(x))
      )

    test_cases = list(
        utils.gen_simple_agg_into_cases(lambda x: arolla.int64(x.size))
    )
    qtype_signatures = frozenset(
        tuple(x.qtype for x in test_case) for test_case in test_cases
    )
    self.assertCountEqual(
        frozenset(pointwise_test_utils.detect_qtype_signatures(size_op)),
        qtype_signatures,
    )

  def test_smoke_mean_op(self):

    @arolla.optools.as_lambda_operator(
        'mean_op',
        qtype_constraints=[
            constraints.expect_array(P2.x),
            constraints.expect_numerics(P2.x),
            *constraints.expect_edge_or_unspecified(
                P2.into, child_side_param=P2.x
            ),
        ],
    )
    def mean_op(x, into=arolla.unspecified()):
      into = M2.core.default_if_unspecified(into, M2.edge.to_scalar(x))
      s = M2.math._sum_sparse(x, into)  # pylint: disable=protected-access
      c = M2.core.cast_values(
          M2.array.count(x, into), M2.qtype.scalar_qtype_of(x)
      )
      return s / c

    def agg_into_scalar_fn(x):
      if x.value_qtype not in arolla.types.NUMERIC_QTYPES:
        return utils.skip_case
      present_values = arolla.abc.invoke_op(
          'array.present_values', (x,)
      ).py_value()
      result_qtype = arolla.types.common_float_qtype(x.value_qtype)
      if present_values:
        return utils.optional(
            sum(present_values) / len(present_values), result_qtype
        )
      return utils.optional(None, result_qtype)

    test_cases = list(utils.gen_simple_agg_into_cases(agg_into_scalar_fn))
    qtype_signatures = frozenset(
        tuple(x.qtype for x in test_case) for test_case in test_cases
    )
    self.assertCountEqual(
        frozenset(pointwise_test_utils.detect_qtype_signatures(mean_op)),
        qtype_signatures,
    )

  def test_error_not_array(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'unsupported array type: OPTIONAL_UNIT'
    ):
      _ = list(
          utils.gen_simple_agg_into_cases(
              lambda _: utils.skip_case,
              base_arrays=[arolla.optional_unit(None)],
          )
      )


if __name__ == '__main__':
  absltest.main()
