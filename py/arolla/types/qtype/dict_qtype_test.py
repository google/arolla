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

"""Tests for dict_qtype."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operator_tests import dict_test_utils
from arolla.types.qtype import dense_array_qtype as rl_dense_array_qtype
from arolla.types.qtype import dict_qtype as rl_dict_qtype
from arolla.types.qtype import optional_qtype as rl_optional_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype


def make_dict_qtype(keys_type, values_type):
  dict_make = arolla_abc.lookup_operator('dict.make')
  annotation_qtype = arolla_abc.lookup_operator('annotation.qtype')
  dict_expr = dict_make(
      annotation_qtype(
          arolla_abc.leaf('keys'),
          rl_dense_array_qtype.make_dense_array_qtype(keys_type),
      ),
      annotation_qtype(
          arolla_abc.leaf('values'),
          rl_dense_array_qtype.make_dense_array_qtype(values_type),
      ),
  )
  assert dict_expr.qtype is not None
  return dict_expr.qtype


def make_key_to_row_dict_qtype(keys_type):
  key_to_row_dict_qtype = arolla_abc.invoke_op(
      'qtype._get_key_to_row_dict_qtype', (keys_type,)
  )
  assert key_to_row_dict_qtype is not None
  return key_to_row_dict_qtype


_DICT_KEY_AND_VALUE_QTYPES = frozenset(
    itertools.product(
        dict_test_utils.DICT_KEY_QTYPES, dict_test_utils.DICT_VALUE_QTYPES
    )
)


class DictQtypeTest(parameterized.TestCase):

  @parameterized.parameters(
      *rl_scalar_qtype.SCALAR_QTYPES, *rl_optional_qtype.OPTIONAL_QTYPES
  )
  def test_is_dict_qtype_false(self, qtype):
    self.assertFalse(rl_dict_qtype.is_dict_qtype(qtype))

  @parameterized.parameters(
      *(make_dict_qtype(k, v) for (k, v) in _DICT_KEY_AND_VALUE_QTYPES)
  )
  def test_is_dict_qtype_true(self, qtype):
    self.assertTrue(rl_dict_qtype.is_dict_qtype(qtype))

  def test_is_dict_qtype_error(self):
    with self.assertRaises(TypeError):
      rl_dict_qtype.is_dict_qtype(rl_scalar_qtype.float64(5))

  @parameterized.parameters(*_DICT_KEY_AND_VALUE_QTYPES)
  def test_make_dict_qtype(self, keys_type, values_type):
    self.assertEqual(
        rl_dict_qtype.make_dict_qtype(keys_type, values_type),
        make_dict_qtype(keys_type, values_type),
    )

  def test_make_dict_qtype_not_qtype_error(self):
    with self.assertRaises(TypeError):
      rl_dict_qtype.make_dict_qtype(
          rl_scalar_qtype.INT32, rl_scalar_qtype.float64(5)
      )

  def test_make_dict_qtype_invalid_dict_qtype_error(self):
    with self.assertRaisesRegex(ValueError, 'no dict with UNIT keys found'):
      rl_dict_qtype.make_dict_qtype(rl_scalar_qtype.UNIT, rl_scalar_qtype.INT32)


class KeyToRowDictQtypeTest(parameterized.TestCase):

  @parameterized.parameters(
      *rl_scalar_qtype.SCALAR_QTYPES, *rl_optional_qtype.OPTIONAL_QTYPES
  )
  def test_is_key_to_row_dict_qtype_false(self, qtype):
    self.assertFalse(rl_dict_qtype.is_key_to_row_dict_qtype(qtype))

  @parameterized.parameters(
      *(make_key_to_row_dict_qtype(k) for k in dict_test_utils.DICT_KEY_QTYPES)
  )
  def test_is_key_to_row_dict_qtype_true(self, qtype):
    self.assertTrue(rl_dict_qtype.is_key_to_row_dict_qtype(qtype))

  def test_is_key_to_row_dict_qtype_error(self):
    with self.assertRaises(TypeError):
      rl_dict_qtype.is_key_to_row_dict_qtype(rl_scalar_qtype.float64(5))

  @parameterized.parameters(*dict_test_utils.DICT_KEY_QTYPES)
  def test_make_key_to_row_dict_qtype(self, key_type):
    self.assertEqual(
        rl_dict_qtype.make_key_to_row_dict_qtype(key_type),
        make_key_to_row_dict_qtype(key_type),
    )

  def test_make_key_to_row_dict_qtype_not_qtype_error(self):
    with self.assertRaises(TypeError):
      rl_dict_qtype.make_key_to_row_dict_qtype(rl_scalar_qtype.float64(5))

  def test_make_key_to_row_dict_qtype_invalid_dict_qtype_error(self):
    with self.assertRaisesRegex(ValueError, 'no dict with UNIT keys found'):
      rl_dict_qtype.make_key_to_row_dict_qtype(rl_scalar_qtype.UNIT)


if __name__ == '__main__':
  absltest.main()
