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

"""Tests for the M.array.make_array_shape operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_cases():
  """Yields test cases for array.make_array_shape.

  Yields: (arg, result)
  """
  int_modifiers = (arolla.int32, arolla.int64)
  for i in (0, 1, 5, 100):
    for int_mod in int_modifiers:
      yield int_mod(i), arolla.types.ArrayShape(i)


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset((arg.qtype, res.qtype) for arg, res in TEST_CASES)


class ArrayMakeArrayShapeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.make_array_shape, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_CASES)
  def test_value(self, arg, expected_result):
    result = self.eval(M.array.make_array_shape(arg))
    arolla.testing.assert_qvalue_allequal(result, expected_result)

  def test_negative_size_error(self):
    # The error message is omitted since it's backend specific.
    with self.assertRaises(ValueError):
      self.eval(M.array.make_array_shape(-1))


if __name__ == '__main__':
  absltest.main()
