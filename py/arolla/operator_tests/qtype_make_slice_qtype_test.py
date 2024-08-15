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

"""Tests for M.qtype.make_slice_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M


class QTypeMakeSliceQType(parameterized.TestCase):

  @parameterized.parameters(
      (),
      (arolla.INT32),
      (arolla.INT32, arolla.FLOAT32),
      (arolla.INT32, arolla.FLOAT32, arolla.types.make_tuple_qtype()),
  )
  def test_eval(self, *input_qtypes):
    slice_qtype = arolla.eval(M.qtype.make_slice_qtype(*input_qtypes))
    self.assertTrue(arolla.eval(M.qtype.is_slice_qtype(slice_qtype)).py_value())
    decayed_qtype = arolla.eval(M.qtype.decay_derived_qtype(slice_qtype))
    self.assertTrue(arolla.is_tuple_qtype(decayed_qtype))
    field_qtypes = arolla.abc.get_field_qtypes(decayed_qtype)
    expected_field_qtypes = input_qtypes + tuple(
        arolla.UNSPECIFIED for _ in range(3 - len(input_qtypes))
    )
    self.assertEqual(field_qtypes, expected_field_qtypes)

  @parameterized.parameters(
      ({'start': 1},),
      ({'stop': 1},),
      ({'step': 1},),
  )
  def test_type_error(self, data):
    with self.assertRaisesRegex(ValueError, 'expected QTYPE, got: INT32'):
      _ = M.qtype.make_slice_qtype(**data)

  @parameterized.parameters(
      (
          {
              'start': M.qtype.conditional_qtype(
                  arolla.missing(), arolla.INT32, arolla.INT64
              )
          },
      ),
      (
          {
              'stop': M.qtype.conditional_qtype(
                  arolla.present(), arolla.INT32, arolla.INT64
              )
          },
      ),
      (
          {
              'step': M.qtype.conditional_qtype(
                  arolla.present(), arolla.INT32, arolla.INT64
              )
          },
      ),
  )
  def test_non_literal_error(self, data):
    with self.assertRaisesRegex(ValueError, 'expected a literal'):
      _ = M.qtype.make_slice_qtype(**data)


if __name__ == '__main__':
  absltest.main()
