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

"""Tests for edge.from_mapping."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_data():
  """Yields test data for edge.from_mapping operator.

  Yields: (mapping_qvalue, parent_size_qvalue)
  """
  array_fns = (
      arolla.dense_array_int32,
      arolla.dense_array_int64,
      arolla.array_int32,
      arolla.array_int64,
  )
  scalar_fns = (
      arolla.int32,
      arolla.int64,
  )
  for array_fn in array_fns:
    for scalar_fn in scalar_fns:
      yield array_fn([]), scalar_fn(0)
      yield array_fn([None]), scalar_fn(0)
      yield array_fn([4, 3, 1, None, 5, 2, None]), scalar_fn(10)


TEST_DATA = tuple(gen_test_data())

# QType signatures for edge.from_mapping.
#
#   ((mapping_qtype, parent_size_qtype, result_qtype), ...)
QTYPE_SIGNATURES = tuple(
    (mapping.qtype, parent_size.qtype, arolla.DENSE_ARRAY_EDGE)
    for mapping, parent_size in TEST_DATA
    if arolla.is_dense_array_qtype(mapping.qtype)
) + tuple(
    (mapping.qtype, parent_size.qtype, arolla.ARRAY_EDGE)
    for mapping, parent_size in TEST_DATA
    if not arolla.is_dense_array_qtype(mapping.qtype)
)


class EdgeFromMappingTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.edge.from_mapping, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_DATA)
  def test_eval(self, mapping_qvalue, parent_size_qvalue):
    actual_edge_qvalue = self.eval(
        M.edge.from_mapping(mapping_qvalue, parent_size_qvalue)
    )
    self.assertListEqual(
        actual_edge_qvalue.mapping().py_value(), mapping_qvalue.py_value()
    )
    self.assertEqual(actual_edge_qvalue.parent_size, parent_size_qvalue)

  def test_error_negative_mapping(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("mapping can't contain negative values")
    ):
      _ = self.eval(M.edge.from_mapping([-1], 10))

  def test_error_negative_parent_size(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('parent_size can not be negative')
    ):
      _ = self.eval(M.edge.from_mapping(arolla.array_int64([]), -1))

  def test_error_mapping_is_out_range(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('parent_size=10, but parent id 100 is used')
    ):
      _ = self.eval(M.edge.from_mapping([100], 10))


if __name__ == '__main__':
  absltest.main()
