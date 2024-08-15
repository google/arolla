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

"""Tests for M.random.cityhash."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  for x, s in itertools.product(
      arolla.types.SCALAR_QTYPES, arolla.types.INTEGRAL_QTYPES
  ):
    for lifted_args in pointwise_test_utils.lift_qtypes((x, arolla.INT64)):
      yield (lifted_args[0], s, lifted_args[1])


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class RandomHashInt30Test(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertCountEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.random.cityhash)
        ),
    )

  def testString(self):
    value = arolla.text('value')
    hash_1 = arolla.eval(M.random.cityhash(value, 123))
    hash_2 = arolla.eval(M.random.cityhash(value, arolla.int64(123)))
    hash_3 = arolla.eval(M.random.cityhash(value, 456))
    hash_4 = arolla.eval(M.random.cityhash(arolla.text('new_value'), 123))
    self.assertEqual(hash_1, hash_2)
    self.assertNotEqual(hash_1, hash_3)
    self.assertNotEqual(hash_1, hash_4)

  def testSameOutputForDifferentTypes(self):
    hash_1 = arolla.eval(M.random.cityhash(123, 123))
    hash_2 = arolla.eval(M.random.cityhash(arolla.int64(123), 123))
    hash_3 = arolla.eval(M.random.cityhash(arolla.float32(123), 123))
    hash_4 = arolla.eval(M.random.cityhash(arolla.float64(123), 123))
    hash_5 = arolla.eval(M.random.cityhash(arolla.text('123'), 123))
    self.assertEqual(hash_1, hash_2)
    self.assertEqual(hash_1, hash_3)
    self.assertEqual(hash_1, hash_4)
    self.assertEqual(hash_1, hash_5)

  def testStringArray(self):
    arr = arolla.array_text(['value1', 'value2'])
    hash_1 = arolla.eval(M.random.cityhash(arr, 123))
    hash_2 = arolla.eval(M.random.cityhash(arr, arolla.int64(123)))
    arolla.testing.assert_qvalue_allequal(hash_1, hash_2)

  def testOptional(self):
    value = arolla.optional_text('value')
    hash_1 = arolla.eval(M.random.cityhash(value, 123))
    hash_2 = arolla.eval(M.random.cityhash(value, arolla.int64(123)))
    hash_3 = arolla.eval(M.random.cityhash(value, 456))
    hash_4 = arolla.eval(
        M.random.cityhash(arolla.optional_text('new_value'), 123)
    )
    hash_5 = arolla.eval(M.random.cityhash(arolla.optional_text(None), 123))
    self.assertEqual(hash_1, hash_2)
    self.assertNotEqual(hash_1, hash_3)
    self.assertNotEqual(hash_1, hash_4)
    self.assertFalse(hash_5.is_present)


if __name__ == '__main__':
  absltest.main()
