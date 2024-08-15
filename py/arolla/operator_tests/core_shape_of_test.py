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

"""Tests for the M.core.shape_of operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


def gen_test_data():
  """Yields test data for core.shape_of.

  Yields: (arg_1, result)
  """
  for _, scalar in utils.SCALAR_VALUES:
    yield scalar, arolla.types.ScalarShape()
  for _, optional_scalar in utils.OPTIONAL_VALUES:
    yield optional_scalar, arolla.types.OptionalScalarShape()
  for i in (0, 1, 5, 1000):
    data = [None] * i
    for qtype in arolla.types.ARRAY_QTYPES:
      yield (
          arolla.array(data, value_qtype=qtype.value_qtype),
          arolla.types.ArrayShape(i),
      )
    for qtype in arolla.types.DENSE_ARRAY_QTYPES:
      yield (
          arolla.dense_array(data, value_qtype=qtype.value_qtype),
          arolla.types.DenseArrayShape(i),
      )


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset((arg.qtype, res.qtype) for arg, res in TEST_DATA)


class CoreShapeOfTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        QTYPE_SIGNATURES,
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.core.shape_of)
        ),
    )

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, expected_result):
    result = self.eval(M.core.shape_of(arg1))
    arolla.testing.assert_qvalue_allequal(result, expected_result)


if __name__ == '__main__':
  absltest.main()
