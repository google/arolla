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

"""Tests for array.iota operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils

M = arolla.M

QTYPE_SIGNATURES = [
    (arolla.SCALAR_SHAPE, arolla.INT64),
    (arolla.OPTIONAL_SCALAR_SHAPE, arolla.OPTIONAL_INT64),
    (arolla.DENSE_ARRAY_SHAPE, arolla.DENSE_ARRAY_INT64),
    (arolla.ARRAY_SHAPE, arolla.ARRAY_INT64),
]


class ArrayIotaTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.array.iota, QTYPE_SIGNATURES
    )

  @parameterized.named_parameters(iter(utils.ARRAY_SHAPE_N_FACTORIES))
  def testEmpty(self, array_shape_n):
    x = utils.pyval(M.array.iota(array_shape_n(0)))
    self.assertEmpty(x)

  def testScalar(self):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.array.iota(arolla.types.ScalarShape())), arolla.int64(0)
    )

  def testOptional(self):
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.array.iota(arolla.types.OptionalScalarShape())),
        arolla.optional_int64(0),
    )

  @parameterized.named_parameters(iter(utils.ARRAY_FACTORIES))
  def testSize10(self, make_array):
    result = make_array(range(10), value_qtype=arolla.INT64)
    shape = arolla.eval(arolla.M.core.shape_of(result))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.array.iota(shape)), result
    )


if __name__ == '__main__':
  absltest.main()
