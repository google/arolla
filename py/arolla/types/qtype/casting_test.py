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
from arolla.abc import abc as rl_abc
from arolla.types.qtype import array_qtype as rl_array_qtype
from arolla.types.qtype import casting as rl_casting
from arolla.types.qtype import dense_array_qtype as rl_dense_array_qtype
from arolla.types.qtype import optional_qtype as rl_optional_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import tuple_qtype as rl_tuple_qtype

_BOOLEAN = rl_scalar_qtype.BOOLEAN
_INT32 = rl_scalar_qtype.INT32
_INT64 = rl_scalar_qtype.INT64
_FLOAT32 = rl_scalar_qtype.FLOAT32
_WEAK_FLOAT = rl_scalar_qtype.WEAK_FLOAT

_OPTIONAL_INT32 = rl_optional_qtype.OPTIONAL_INT32
_OPTIONAL_FLOAT32 = rl_optional_qtype.OPTIONAL_FLOAT32
_OPTIONAL_FLOAT64 = rl_optional_qtype.OPTIONAL_FLOAT64
_OPTIONAL_WEAK_FLOAT = rl_optional_qtype.OPTIONAL_WEAK_FLOAT

_ARRAY_BYTES = rl_array_qtype.ARRAY_BYTES
_ARRAY_INT32 = rl_array_qtype.ARRAY_INT32
_ARRAY_INT64 = rl_array_qtype.ARRAY_INT64
_ARRAY_FLOAT32 = rl_array_qtype.ARRAY_FLOAT32
_ARRAY_FLOAT64 = rl_array_qtype.ARRAY_FLOAT64

_DENSE_ARRAY_INT32 = rl_dense_array_qtype.DENSE_ARRAY_INT32

_EMPTY_TUPLE = rl_tuple_qtype.make_tuple_qtype()

_NOTHING = rl_abc.NOTHING


class CastingTest(parameterized.TestCase):

  def testQTypeError(self):
    self.assertTrue(issubclass(rl_casting.QTypeError, ValueError))

  @parameterized.parameters(
      ([_INT32], _INT32),
      ([_INT32, _INT32], _INT32),
      ([_INT32, _INT64], _INT64),
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
  def testCommonQType(self, qtypes, expected_qtype):
    actual_qtype = rl_casting.common_qtype(*qtypes)
    self.assertEqual(actual_qtype, expected_qtype)

  @parameterized.parameters(
      (),
      (_INT32, object()),
  )
  def testCommonQTypes_TypeError(self, *inputs):
    with self.assertRaises(TypeError):
      _ = rl_casting.common_qtype(*inputs)

  @parameterized.parameters(
      (_INT32, _BOOLEAN),
      (_INT32, _FLOAT32),
      (_INT32, _WEAK_FLOAT),
      (_ARRAY_INT32, _DENSE_ARRAY_INT32),
      (_NOTHING, _EMPTY_TUPLE),
  )
  def testCommonQTypes_QTypeError(self, *inputs):
    with self.assertRaises(rl_casting.QTypeError):
      _ = rl_casting.common_qtype(*inputs)

  @parameterized.parameters(
      ([], _INT32, _INT32),
      ([_OPTIONAL_FLOAT32], _INT32, _OPTIONAL_INT32),
      ([_OPTIONAL_FLOAT32, _ARRAY_BYTES], _INT32, _ARRAY_INT32),
  )
  def testBroadcastQType(self, target_qtypes, qtype, expected_result):
    self.assertEqual(
        rl_casting.broadcast_qtype(target_qtypes, qtype), expected_result
    )

  @parameterized.parameters(
      ([], _EMPTY_TUPLE),
      ([_ARRAY_INT32], _DENSE_ARRAY_INT32),
  )
  def testBroadcastQType_QTypeError(self, target_qtypes, qtype):
    with self.assertRaises(rl_casting.QTypeError):
      _ = rl_casting.broadcast_qtype(target_qtypes, qtype)

  @parameterized.parameters(
      ([object()], _EMPTY_TUPLE),
      ([_ARRAY_INT32], object()),
  )
  def testBroadcastQType_TypeError(self, *inputs):
    with self.assertRaises(TypeError):
      _ = rl_casting.broadcast_qtype(*inputs)


if __name__ == '__main__':
  absltest.main()
