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

"""Tests for M.namedtuple.make."""

import inspect

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M

TEST_DATA = (
    {},
    dict(a=arolla.unit()),
    dict(a=arolla.unit(), b=arolla.boolean(False)),
    dict(a=arolla.unit(), b=arolla.boolean(False), c=arolla.bytes(b'foo')),
)


class NamedtupleMakeTest(parameterized.TestCase):

  def test_inspect_signature(self):
    expected_signature = inspect.signature(lambda *args, **fields: None)
    self.assertEqual(inspect.signature(M.namedtuple.make), expected_signature)

  @parameterized.parameters(*TEST_DATA)
  def test_classic_mode(self, **fields):
    actual_value = arolla.eval(
        M.namedtuple.make(','.join(fields.keys()), *fields.values())
    )
    for f in fields:
      arolla.testing.assert_qvalue_allequal(actual_value[f], fields[f])

  @parameterized.parameters(*TEST_DATA)
  def test_kwargs_mode(self, **fields):
    actual_value = arolla.eval(M.namedtuple.make(**fields))
    for f in fields:
      arolla.testing.assert_qvalue_allequal(actual_value[f], fields[f])

  @parameterized.parameters(*TEST_DATA)
  def test_supports_tuple_of_field_names(self, **fields):
    actual_value = arolla.eval(
        M.namedtuple.make(tuple(fields.keys()), *fields.values())
    )
    for f in fields:
      arolla.testing.assert_qvalue_allequal(actual_value[f], fields[f])

  @parameterized.parameters(*TEST_DATA)
  def test_supports_byte_array_of_field_names(self, **fields):
    keys = arolla.dense_array_bytes([k.encode('utf-8') for k in fields.keys()])
    actual_value = arolla.eval(M.namedtuple.make(keys, *fields.values()))
    for f in fields:
      arolla.testing.assert_qvalue_allequal(actual_value[f], fields[f])

  def test_type_checks_field_names(self):
    with self.assertRaisesRegex(ValueError, 'all field_names must be TEXTs'):
      M.namedtuple.make(
          (arolla.bytes(b'x'), arolla.text('y')),
          (arolla.float32(2.0), arolla.int32(3)),
      )


if __name__ == '__main__':
  absltest.main()
