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

"""Tests for M.edge.to_scalar."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M


def gen_qtype_signatures():
  """Yields qtype signatures for edge.to_scalar.

  Yields: (arg_qtype, result_qtype)
  """
  yield from (
      (qtype, arolla.types.SCALAR_TO_SCALAR_EDGE)
      for qtype in arolla.types.SCALAR_QTYPES + arolla.types.OPTIONAL_QTYPES
  )
  yield from (
      (qtype, arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE)
      for qtype in arolla.types.DENSE_ARRAY_QTYPES
  )
  yield from (
      (qtype, arolla.ARRAY_TO_SCALAR_EDGE)
      for qtype in arolla.types.ARRAY_QTYPES
  )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class EdgeToScalar(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(M.edge.to_scalar, QTYPE_SIGNATURES)

  def test_scalar_to_scalar_edge(self):
    with self.subTest('scalar'):
      output_qvalue = arolla.eval(M.edge.to_scalar(arolla.int32(1)))
      self.assertEqual(output_qvalue.qtype, arolla.types.SCALAR_TO_SCALAR_EDGE)
    with self.subTest('optional_scalar'):
      output_qvalue = arolla.eval(M.edge.to_scalar(arolla.optional_int32(1)))
      self.assertEqual(output_qvalue.qtype, arolla.types.SCALAR_TO_SCALAR_EDGE)

  @parameterized.parameters(0, 1, 100)
  def test_array_to_scalar_edge(self, n):
    with self.subTest('dense_array'):
      input_qvalue = arolla.eval(arolla.dense_array_int32([None] * n))
      output_qvalue = arolla.eval(M.edge.to_scalar(input_qvalue))
      self.assertEqual(
          output_qvalue.qtype, arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE
      )
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.parent_shape(output_qvalue)),
          arolla.types.OptionalScalarShape(),
      )
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.child_shape(output_qvalue)),
          arolla.types.DenseArrayShape(n),
      )

    with self.subTest('array'):
      input_qvalue = arolla.eval(arolla.array_int32([None] * n))
      output_qvalue = arolla.eval(M.edge.to_scalar(input_qvalue))
      self.assertEqual(output_qvalue.qtype, arolla.ARRAY_TO_SCALAR_EDGE)
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.parent_shape(output_qvalue)),
          arolla.types.OptionalScalarShape(),
      )
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.child_shape(output_qvalue)),
          arolla.types.ArrayShape(n),
      )


if __name__ == '__main__':
  absltest.main()
