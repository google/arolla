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

"""Tests for M.qtype.scalar_qtype_of operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


class QTypeScalarQTypeOfTest(parameterized.TestCase):

  @parameterized.parameters(
      (arolla.int32(0), arolla.INT32),
      (arolla.optional_int64(None), arolla.INT64),
      (arolla.dense_array_float32([]), arolla.FLOAT32),
      (arolla.array_bytes([b'foo']), arolla.BYTES),
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.scalar_qtype_of(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  def test_no_scalar_qtype_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a type storing scalar(s), got arg: tuple<>'),
    ):
      _ = M.qtype.scalar_qtype_of(arolla.tuple())


if __name__ == '__main__':
  absltest.main()
