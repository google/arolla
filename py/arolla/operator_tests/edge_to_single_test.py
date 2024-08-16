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

"""Tests for M.edge.to_single."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  """Yields qtype signatures for edge.to_single.

  Yields: (arg_qtype, result_qtype)
  """
  yield from (
      (qtype, arolla.types.DENSE_ARRAY_EDGE)
      for qtype in arolla.types.DENSE_ARRAY_QTYPES
  )
  yield from ((qtype, arolla.ARRAY_EDGE) for qtype in arolla.types.ARRAY_QTYPES)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class EdgeToSingleTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.to_single, QTYPE_SIGNATURES)

  @parameterized.named_parameters(
      utils.product_parameters(
          utils.CONST_ARRAY_FACTORIES,
          utils.SCALAR_VALUES + utils.OPTIONAL_VALUES,
          utils.SIZES,
      )
  )
  def testFromSize(self, gen_const_array, value, size):
    e = self.eval(M.edge.to_single(gen_const_array(value, size)))
    self.assertEqual(e.child_size, size)

  @parameterized.named_parameters(
      utils.product_parameters(
          utils.CONST_ARRAY_FACTORIES,
          utils.SCALAR_VALUES + utils.OPTIONAL_VALUES,
          utils.SIZES,
      )
  )
  def testParentSize(self, gen_const_array, value, size):
    e = self.eval(M.edge.to_single(gen_const_array(value, size)))
    self.assertEqual(e.parent_size, 1)

  @parameterized.named_parameters(
      utils.product_parameters(
          utils.CONST_ARRAY_FACTORIES,
          utils.SCALAR_VALUES + utils.OPTIONAL_VALUES,
          utils.SIZES,
      )
  )
  def testMapping_ConstArray(self, gen_const_array, value, size):
    e = self.eval(M.edge.to_single(gen_const_array(value, size)))
    self.assertEqual(arolla.eval(M.array.count(M.edge.mapping(e) == 0)), size)

  def testErrorMessage(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an array type, got array: INT32')
    ):
      _ = M.edge.to_single(1)


if __name__ == '__main__':
  absltest.main()
