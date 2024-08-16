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

"""Tests for the M.edge.from_sizes_or_shape operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_data():
  """Yields test data for edge.from_sizes_or_shape.

  Yields: (arg_1, result)
  """
  # Sizes.
  for array_mut, int_qtype in itertools.product(
      (arolla.dense_array, arolla.array), arolla.types.INTEGRAL_QTYPES
  ):
    sizes = array_mut([0, 1, 2, 0], value_qtype=int_qtype)
    yield (sizes, arolla.eval(M.edge.from_sizes(sizes)))

  # Shapes.
  for shape in (
      arolla.types.ArrayShape(3),
      arolla.types.DenseArrayShape(3),
      arolla.types.ScalarShape(),
      arolla.types.OptionalScalarShape(),
  ):
    yield (shape, arolla.eval(M.edge.from_shape(shape)))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = frozenset((arg.qtype, res.qtype) for arg, res in TEST_DATA)


class EdgeFromSizesOrShapeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.edge.from_sizes_or_shape, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg1, expected_result):
    result = self.eval(M.edge.from_sizes_or_shape(arg1))
    arolla.testing.assert_qvalue_equal_by_fingerprint(result, expected_result)


if __name__ == '__main__':
  absltest.main()
