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

"""Tests for edge.from_sizes."""

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


class EdgeFromSizesTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.from_sizes, QTYPE_SIGNATURES)

  @parameterized.parameters(
      (arolla.dense_array_int32([]), arolla.types.DENSE_ARRAY_EDGE),
      (arolla.dense_array_int64([]), arolla.types.DENSE_ARRAY_EDGE),
      (arolla.dense_array_int32([1, 2, 1]), arolla.types.DENSE_ARRAY_EDGE),
      (arolla.dense_array_int64([1, 2, 1]), arolla.types.DENSE_ARRAY_EDGE),
      (arolla.array_int32([]), arolla.ARRAY_EDGE),
      (arolla.array_int64([]), arolla.ARRAY_EDGE),
      (arolla.array_int32([1, 2, 1]), arolla.ARRAY_EDGE),
      (arolla.array_int64([1, 2, 1]), arolla.ARRAY_EDGE),
  )
  def test_qvalues(self, array_qvalue, expected_edge_qtype):
    expected_mapping = []
    for i, size in enumerate(array_qvalue.py_value()):
      expected_mapping += [i] * size
    output_qvalue = self.eval(M.edge.from_sizes(array_qvalue))
    self.assertEqual(output_qvalue.qtype, expected_edge_qtype)
    self.assertEqual(output_qvalue.parent_size, array_qvalue.size)
    self.assertEqual(output_qvalue.child_size, sum(array_qvalue.py_value()))
    self.assertListEqual(output_qvalue.mapping().py_value(), expected_mapping)

  def test_missing_size_error(self):
    with self.assertRaises(Exception):
      _ = self.eval(M.edge.from_sizes([1, None]))

  def test_negative_size_error(self):
    with self.assertRaises(Exception):
      _ = self.eval(M.edge.from_sizes([1, -1]))


if __name__ == '__main__':
  absltest.main()
