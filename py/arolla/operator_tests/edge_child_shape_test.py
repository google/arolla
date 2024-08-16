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

"""Tests for the M.edge.child_shape operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_data():
  """Yields test data for edge.child_shape.

  Yields: (arg_1, result)
  """
  yield from [
      (
          arolla.types.ArrayEdge.from_sizes([1, 2, 0, 1, 0]),
          arolla.types.ArrayShape(4),
      ),
      (
          arolla.types.DenseArrayEdge.from_sizes([1, 2, 0, 1, 0]),
          arolla.types.DenseArrayShape(4),
      ),
      (arolla.types.ArrayToScalarEdge(3), arolla.types.ArrayShape(3)),
      (arolla.types.DenseArrayToScalarEdge(3), arolla.types.DenseArrayShape(3)),
      (arolla.types.ScalarToScalarEdge(), arolla.types.OptionalScalarShape()),
  ]


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset((arg.qtype, res.qtype) for arg, res in TEST_DATA)


class EdgeChildShapeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.child_shape, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, expected_result):
    result = self.eval(M.edge.child_shape(arg1))
    arolla.testing.assert_qvalue_allequal(result, expected_result)


if __name__ == '__main__':
  absltest.main()
