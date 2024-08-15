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

"""Tests for the M.array.array_shape_size operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_data():
  """Yields test data for array.array_shape_size.

  Yields: (arg_1, result)
  """
  for i in (0, 1, 5, 1000, 2**45):
    yield (arolla.types.ArrayShape(i), arolla.int64(i))
    yield (arolla.types.DenseArrayShape(i), arolla.int64(i))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset((arg.qtype, res.qtype) for arg, res in TEST_DATA)


class ArrayArrayShapeSizeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.array_shape_size, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, expected_result):
    result = self.eval(M.array.array_shape_size(arg1))
    arolla.testing.assert_qvalue_allequal(result, expected_result)


if __name__ == '__main__':
  absltest.main()
