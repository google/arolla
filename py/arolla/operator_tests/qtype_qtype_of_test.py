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

"""Tests for M.qtype.qtype_of operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


class QTypeQTypeOfTest(parameterized.TestCase):

  @parameterized.parameters(
      (arolla.int32(0), arolla.INT32),
      (arolla.optional_int64(None), arolla.OPTIONAL_INT64),
      (arolla.dense_array_float32([]), arolla.DENSE_ARRAY_FLOAT32),
      (arolla.array_bytes([b'foo']), arolla.ARRAY_BYTES),
      (arolla.tuple(), arolla.make_tuple_qtype()),
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.qtype_of(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(
      arolla.INT32,
      arolla.OPTIONAL_INT64,
      arolla.DENSE_ARRAY_FLOAT32,
      arolla.ARRAY_BYTES,
      arolla.make_tuple_qtype(),
  )
  def test_to_lower(self, qtype):
    actual_value = arolla.abc.to_lower_node(
        M.qtype.qtype_of(M.annotation.qtype(L.arg, qtype))
    ).qvalue
    arolla.testing.assert_qvalue_allequal(actual_value, qtype)

  def test_to_lower_no_type(self):
    expr = M.qtype.qtype_of(L.x)
    self.assertEqual(
        expr.fingerprint, arolla.abc.to_lower_node(expr).fingerprint
    )


if __name__ == '__main__':
  absltest.main()
