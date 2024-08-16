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

"""Tests for edge.from_shape."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M

QTYPE_SIGNATURES = (
    (arolla.DENSE_ARRAY_SHAPE, arolla.DENSE_ARRAY_TO_SCALAR_EDGE),
    (arolla.ARRAY_SHAPE, arolla.ARRAY_TO_SCALAR_EDGE),
    (arolla.SCALAR_SHAPE, arolla.types.SCALAR_TO_SCALAR_EDGE),
    (arolla.OPTIONAL_SCALAR_SHAPE, arolla.types.SCALAR_TO_SCALAR_EDGE),
)


class EdgeFromShapeTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.from_shape, QTYPE_SIGNATURES)

  def test_scalar_to_scalar_edge(self):
    with self.subTest('scalar'):
      output_qvalue = self.eval(M.edge.from_shape(arolla.types.ScalarShape()))
      self.assertEqual(output_qvalue.qtype, arolla.types.SCALAR_TO_SCALAR_EDGE)
    with self.subTest('optional_scalar'):
      output_qvalue = self.eval(
          M.edge.from_shape(arolla.types.OptionalScalarShape())
      )
      self.assertEqual(output_qvalue.qtype, arolla.types.SCALAR_TO_SCALAR_EDGE)

  @parameterized.parameters(0, 1, 100)
  def test_array_to_scalar_edge(self, n):
    with self.subTest('dense_array'):
      dense_array_shape = arolla.types.DenseArrayShape(n)
      output_qvalue = self.eval(M.edge.from_shape(dense_array_shape))
      self.assertEqual(
          output_qvalue.qtype, arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE
      )
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.parent_shape(output_qvalue)),
          arolla.types.OptionalScalarShape(),
      )
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.child_shape(output_qvalue)), dense_array_shape
      )

    with self.subTest('array'):
      array_shape = arolla.types.ArrayShape(n)
      output_qvalue = self.eval(M.edge.from_shape(array_shape))
      self.assertEqual(output_qvalue.qtype, arolla.ARRAY_TO_SCALAR_EDGE)
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.parent_shape(output_qvalue)),
          arolla.types.OptionalScalarShape(),
      )
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.edge.child_shape(output_qvalue)), array_shape
      )


if __name__ == '__main__':
  absltest.main()
