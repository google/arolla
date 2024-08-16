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

"""Tests for edge.from_split_points."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M

QTYPE_SIGNATURES = (
    (arolla.DENSE_ARRAY_INT32, arolla.DENSE_ARRAY_EDGE),
    (arolla.DENSE_ARRAY_INT64, arolla.DENSE_ARRAY_EDGE),
    (arolla.ARRAY_INT32, arolla.ARRAY_EDGE),
    (arolla.ARRAY_INT64, arolla.ARRAY_EDGE),
)


class EdgeFromSplitPointsTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.edge.from_split_points, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      arolla.dense_array_int32([0]),
      arolla.dense_array_int64([0]),
      arolla.dense_array_int32([0, 1, 3, 4]),
      arolla.dense_array_int64([0, 1, 4, 5]),
      arolla.array_int32([0]),
      arolla.array_int64([0]),
      arolla.array_int32([0, 1, 3, 4]),
      arolla.array_int64([0, 1, 4, 5]),
  )
  def test_eval(self, arg_qvalue):
    split_points = arg_qvalue.py_value()
    expected_parent_size = len(split_points) - 1
    expected_child_size = split_points[-1]
    expected_mapping = []
    for i in range(len(split_points) - 1):
      expected_mapping += [i] * (split_points[i + 1] - split_points[i])
    actual_edge_qvalue = self.eval(M.edge.from_split_points(arg_qvalue))
    self.assertListEqual(
        actual_edge_qvalue.mapping().py_value(), expected_mapping
    )
    self.assertEqual(actual_edge_qvalue.parent_size, expected_parent_size)
    self.assertEqual(actual_edge_qvalue.child_size, expected_child_size)

  def test_error_no_split_points(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('split points array should have at least 1 element'),
    ):
      _ = self.eval(M.edge.from_split_points(arolla.array_int64([])))

  def test_error_missing_split_point(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('split points should be full')
    ):
      _ = self.eval(M.edge.from_split_points([0, None]))

  def test_error_non_monotonic_split_point(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('split points should be sorted')
    ):
      _ = self.eval(M.edge.from_split_points([0, 1, 0]))


if __name__ == '__main__':
  absltest.main()
