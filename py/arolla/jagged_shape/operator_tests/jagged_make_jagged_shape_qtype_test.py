# Copyright 2025 Google LLC
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

"""Tests for M.jagged.make_jagged_shape_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape

M = arolla.M | jagged_shape.M  # pyrefly: ignore[unsupported-operation]


class JaggedMakeJaggedShapeQType(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.jagged.make_jagged_shape_qtype, ((arolla.QTYPE, arolla.QTYPE),)  # pyrefly: ignore[missing-attribute]
    )

  @parameterized.parameters(
      (arolla.ARRAY_EDGE, 'JAGGED_ARRAY_SHAPE'),
      (arolla.DENSE_ARRAY_EDGE, 'JAGGED_DENSE_ARRAY_SHAPE'),
  )
  def test_eval(self, edge_qtype, expected_repr):
    result = arolla.eval(M.jagged.make_jagged_shape_qtype(edge_qtype))  # pyrefly: ignore[missing-attribute]
    self.assertEqual(result.qtype, arolla.QTYPE)
    self.assertNotEqual(result, arolla.NOTHING)
    self.assertEqual(repr(result), expected_repr)  # Proxy to test the type.

  def test_type_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected QTYPE, got edge_qtype: INT32'
    ):
      _ = M.jagged.make_jagged_shape_qtype(1)  # pyrefly: ignore[missing-attribute]

  @parameterized.parameters(
      arolla.ARRAY_INT32, arolla.INT32, arolla.ARRAY_TO_SCALAR_EDGE
  )
  def test_non_array_edge_nothing(self, qtype):
    self.assertEqual(
        arolla.eval(M.jagged.make_jagged_shape_qtype(qtype)), arolla.NOTHING  # pyrefly: ignore[missing-attribute]
    )


if __name__ == '__main__':
  absltest.main()
