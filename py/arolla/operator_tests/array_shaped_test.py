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

"""Tests for M.array.shaped."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  for arg in arolla.types.SCALAR_QTYPES:
    yield arg, arolla.types.ARRAY_SHAPE, arolla.make_array_qtype(arg)
    yield arg, arolla.types.DENSE_ARRAY_SHAPE, arolla.make_dense_array_qtype(
        arg
    )
  for arg in arolla.types.ARRAY_QTYPES + arolla.types.DENSE_ARRAY_QTYPES:
    value_qtype = arg.value_qtype
    yield arg, arolla.types.ARRAY_SHAPE, arolla.make_array_qtype(value_qtype)
    yield arg, arolla.types.DENSE_ARRAY_SHAPE, arolla.make_dense_array_qtype(
        value_qtype
    )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())

values = (None, False, True, b'', b'foo', '', 'bar', 0, 1, 1.5, float('nan'))
TEST_DATA = tuple(zip(values, values))


class Shaped(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.array.shaped, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          TEST_DATA, *((x, y) for x, _, y in QTYPE_SIGNATURES)
      )
  )
  def test(self, arg, expected):
    shape = M.core.shape_of(expected)
    result = arolla.eval(M.array.shaped(arg, shape))
    arolla.testing.assert_qvalue_allequal(result, expected)

  def testScalarToLongerArray(self):
    result = arolla.eval(
        M.array.shaped(4.0, M.core.shape_of(arolla.array([1, 2, 3])))
    )
    arolla.testing.assert_qvalue_allequal(arolla.array([4.0, 4.0, 4.0]), result)
    result = arolla.eval(
        M.array.shaped(4.0, M.core.shape_of(arolla.dense_array([1, 2, 3])))
    )
    arolla.testing.assert_qvalue_allequal(
        arolla.dense_array([4.0, 4.0, 4.0]), result
    )

  def testErrorNonScalarBasedX(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a type storing scalar(s), got x: tuple<>'),
    ):
      _ = M.array.shaped(arolla.tuple(), M.core.shape_of(1))


if __name__ == '__main__':
  absltest.main()
