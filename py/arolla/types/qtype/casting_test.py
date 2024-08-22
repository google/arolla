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

"""Tests for arolla.types.qtype.casting."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import casting
from arolla.types.qtype import dense_array_qtypes
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes

_BOOLEAN = scalar_qtypes.BOOLEAN
_INT32 = scalar_qtypes.INT32
_INT64 = scalar_qtypes.INT64
_BYTES = scalar_qtypes.BYTES
_FLOAT32 = scalar_qtypes.FLOAT32
_FLOAT64 = scalar_qtypes.FLOAT64
_WEAK_FLOAT = scalar_qtypes.WEAK_FLOAT

_OPTIONAL_INT32 = optional_qtypes.OPTIONAL_INT32
_OPTIONAL_FLOAT32 = optional_qtypes.OPTIONAL_FLOAT32
_OPTIONAL_FLOAT64 = optional_qtypes.OPTIONAL_FLOAT64
_OPTIONAL_WEAK_FLOAT = optional_qtypes.OPTIONAL_WEAK_FLOAT

_ARRAY_BYTES = array_qtypes.ARRAY_BYTES
_ARRAY_INT32 = array_qtypes.ARRAY_INT32
_ARRAY_INT64 = array_qtypes.ARRAY_INT64
_ARRAY_FLOAT32 = array_qtypes.ARRAY_FLOAT32
_ARRAY_FLOAT64 = array_qtypes.ARRAY_FLOAT64

_DENSE_ARRAY_INT32 = dense_array_qtypes.DENSE_ARRAY_INT32
_DENSE_ARRAY_FLOAT32 = dense_array_qtypes.DENSE_ARRAY_FLOAT32

_EMPTY_TUPLE = tuple_qtypes.make_tuple_qtype()

_NOTHING = arolla_abc.NOTHING


class CastingTest(parameterized.TestCase):

  def test_qtype_error(self):
    self.assertTrue(issubclass(casting.QTypeError, ValueError))

  @parameterized.parameters(
      ([_INT32], _INT32),
      ([_INT32, _INT32], _INT32),
      ([_INT32, _INT64], _INT64),
      ([_INT32, _FLOAT32], _FLOAT32),
      ([_INT64, _FLOAT32], _FLOAT32),
      ([_INT32, _FLOAT64], _FLOAT64),
      ([_OPTIONAL_INT32, _INT32], _OPTIONAL_INT32),
      ([_OPTIONAL_INT32, _ARRAY_INT32], _ARRAY_INT32),
      ([_OPTIONAL_INT32, _DENSE_ARRAY_INT32], _DENSE_ARRAY_INT32),
      ([_OPTIONAL_INT32, _ARRAY_INT32, _INT64], _ARRAY_INT64),
      ([_OPTIONAL_FLOAT64, _ARRAY_FLOAT32, _WEAK_FLOAT], _ARRAY_FLOAT64),
      ([_FLOAT32, _OPTIONAL_WEAK_FLOAT], _OPTIONAL_FLOAT32),
      ([_EMPTY_TUPLE], _EMPTY_TUPLE),
      ([_EMPTY_TUPLE, _EMPTY_TUPLE], _EMPTY_TUPLE),
      ([_NOTHING, _NOTHING], _NOTHING),
  )
  def test_common_qtype(self, qtypes, expected_qtype):
    actual_qtype = casting.common_qtype(*qtypes)
    self.assertEqual(actual_qtype, expected_qtype)

  @parameterized.parameters(
      (),
      (_INT32, object()),
  )
  def test_common_qtype_type_error(self, *inputs):
    with self.assertRaises(TypeError):
      _ = casting.common_qtype(*inputs)

  @parameterized.parameters(
      (_INT32, _BOOLEAN),
      (_INT32, _BYTES),
      (_BYTES, _WEAK_FLOAT),
      (_ARRAY_INT32, _DENSE_ARRAY_INT32),
      (_NOTHING, _EMPTY_TUPLE),
  )
  def test_common_qtype_qtype_error(self, *inputs):
    with self.assertRaises(casting.QTypeError):
      _ = casting.common_qtype(*inputs)

  @parameterized.parameters(
      ([_INT32], _FLOAT32),
      ([_INT32, _INT64], _FLOAT32),
      ([_INT32, _FLOAT32], _FLOAT32),
      ([_INT32, _FLOAT64], _FLOAT64),
      ([_INT32, _WEAK_FLOAT], _FLOAT32),
      ([_FLOAT32, _WEAK_FLOAT], _FLOAT32),
      ([_WEAK_FLOAT, _WEAK_FLOAT], _WEAK_FLOAT),
      ([_OPTIONAL_INT32, _INT32], _OPTIONAL_FLOAT32),
      ([_OPTIONAL_INT32, _ARRAY_INT32], _ARRAY_FLOAT32),
      ([_OPTIONAL_INT32, _DENSE_ARRAY_INT32], _DENSE_ARRAY_FLOAT32),
      ([_OPTIONAL_INT32, _ARRAY_INT32, _INT64], _ARRAY_FLOAT32),
      ([_OPTIONAL_FLOAT64, _ARRAY_FLOAT32, _WEAK_FLOAT], _ARRAY_FLOAT64),
      ([_FLOAT32, _OPTIONAL_WEAK_FLOAT], _OPTIONAL_FLOAT32),
  )
  def test_common_float_qtype(self, qtypes, expected_qtype):
    actual_qtype = casting.common_float_qtype(*qtypes)
    self.assertEqual(actual_qtype, expected_qtype)

  @parameterized.parameters(
      (_EMPTY_TUPLE),
      (_NOTHING, _NOTHING),
  )
  def test_common_float_qtype_type_error(self, *inputs):
    with self.assertRaises(casting.QTypeError):
      _ = casting.common_float_qtype(*inputs)

  def test_common_float_qtype_no_args(self):
    with self.assertRaises(TypeError):
      _ = casting.common_float_qtype()

  @parameterized.parameters(
      ([], _INT32, _INT32),
      ([_OPTIONAL_FLOAT32], _INT32, _OPTIONAL_INT32),
      ([_OPTIONAL_FLOAT32, _ARRAY_BYTES], _INT32, _ARRAY_INT32),
  )
  def testBroadcastQType(self, target_qtypes, qtype, expected_result):
    self.assertEqual(
        casting.broadcast_qtype(target_qtypes, qtype), expected_result
    )

  @parameterized.parameters(
      ([], _EMPTY_TUPLE),
      ([_ARRAY_INT32], _DENSE_ARRAY_INT32),
  )
  def test_broadcast_qtype_qtype_error(self, target_qtypes, qtype):
    with self.assertRaises(casting.QTypeError):
      _ = casting.broadcast_qtype(target_qtypes, qtype)

  @parameterized.parameters(
      ([object()], _EMPTY_TUPLE),
      ([_ARRAY_INT32], object()),
  )
  def test_broadcast_qtype_type_error(self, *inputs):
    with self.assertRaises(TypeError):
      _ = casting.broadcast_qtype(*inputs)


if __name__ == '__main__':
  absltest.main()
