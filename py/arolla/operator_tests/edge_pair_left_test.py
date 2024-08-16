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

"""Tests for edge.pair_left."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M

QTYPE_SIGNATURES = (
    (arolla.DENSE_ARRAY_INT32, arolla.DENSE_ARRAY_EDGE),
    (arolla.DENSE_ARRAY_INT64, arolla.DENSE_ARRAY_EDGE),
    (arolla.ARRAY_INT32, arolla.ARRAY_EDGE),
    (arolla.ARRAY_INT64, arolla.ARRAY_EDGE),
)


class EdgePairLeftTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.pair_left, QTYPE_SIGNATURES)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testSimple(self, array_factory):
    sizes = array_factory([1, 2, 1])
    expected = arolla.eval(
        M.edge.from_mapping(array_factory([0, 1, 1, 2, 2, 3]), 4)
    )
    actual = self.eval(M.edge.pair_left(sizes))
    self.assertEqual(expected.qtype, actual.qtype)
    self.assertEqual(expected.parent_size, actual.parent_size)
    self.assertEqual(expected.child_size, actual.child_size)
    self.assertListEqual(
        expected.mapping().py_value(), actual.mapping().py_value()
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testEmpty(self, array_factory):
    sizes = array_factory([], arolla.INT32)
    expected = arolla.eval(
        M.edge.from_mapping(array_factory([], arolla.INT32), 0)
    )
    actual = self.eval(M.edge.pair_left(sizes))
    self.assertEqual(expected.qtype, actual.qtype)
    self.assertEqual(expected.parent_size, actual.parent_size)
    self.assertEqual(expected.child_size, actual.child_size)
    self.assertListEqual(
        expected.mapping().py_value(), actual.mapping().py_value()
    )

  def test_missing_size_error(self):
    with self.assertRaises(Exception):
      _ = self.eval(M.edge.pair_left([1, None]))


if __name__ == '__main__':
  absltest.main()
