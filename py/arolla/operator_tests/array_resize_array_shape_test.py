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

"""Tests for the M.array.resize_array_shape operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_data():
  """Yields test data for array.resize_array_shape.

  Yields: (arg_1, arg_2, result)
  """
  int_modifiers = (arolla.int32, arolla.int64)
  shape_modifiers = (arolla.types.ArrayShape, arolla.types.DenseArrayShape)
  for i, j in itertools.product((0, 1, 5, 100), repeat=2):
    for shape_mod, int_mod in itertools.product(shape_modifiers, int_modifiers):
      yield (shape_mod(i), int_mod(j), shape_mod(j))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset(
    (arg1.qtype, arg2.qtype, res.qtype) for arg1, arg2, res in TEST_DATA
)


class ArrayResizeArrayShapeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.resize_array_shape, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, arg2, expected_result):
    result = self.eval(M.array.resize_array_shape(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(result, expected_result)


if __name__ == '__main__':
  absltest.main()
