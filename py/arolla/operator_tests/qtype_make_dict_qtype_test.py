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

"""Tests for M.qtype.make_dict_qtype operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
L = arolla.L

TEST_DATA = (
    (arolla.INT32, arolla.FLOAT32),
    (arolla.INT64, arolla.FLOAT64),
    (arolla.BYTES, arolla.INT32),
    (arolla.TEXT, arolla.INT64),
)

NO_DICT_TEST_DATA = (
    (arolla.FLOAT32, arolla.INT32),
    (arolla.FLOAT64, arolla.INT64),
)


def _gen_test_cases():
  for key_qtype, value_qtype in TEST_DATA:
    yield (
        key_qtype,
        value_qtype,
        arolla.types.make_dict_qtype(key_qtype, value_qtype),
    )
  for key_qtype, value_qtype in NO_DICT_TEST_DATA:
    yield (key_qtype, value_qtype, arolla.NOTHING)


class QTypeMakeDictQType(parameterized.TestCase):

  @parameterized.parameters(*_gen_test_cases())
  def test_value(self, key_qtype, value_qtype, expected_dict_qtype):
    dict_qtype = arolla.eval(
        M.qtype.make_dict_qtype(L.k, L.v),
        # Passing inputs as leaves to prevent evaluation during InferAttributes.
        k=key_qtype,
        v=value_qtype,
    )
    arolla.testing.assert_qvalue_allequal(dict_qtype, expected_dict_qtype)

    # Test InferAttributes.
    expr = M.qtype.make_dict_qtype(key_qtype, value_qtype)
    self.assertIsNotNone(expr.qvalue)
    arolla.testing.assert_qvalue_allequal(expr.qvalue, dict_qtype)

    if expected_dict_qtype != arolla.NOTHING:
      # arolla.types.make_dict_qtype may be implemented using
      # qtype.make_dict_qtype, the test above is not sufficient.
      # Checking key and value QTypes as well.
      arolla.testing.assert_qvalue_allequal(
          arolla.eval(M.qtype.get_dict_key_qtype(dict_qtype)), key_qtype
      )
      arolla.testing.assert_qvalue_allequal(
          arolla.eval(M.qtype.get_dict_value_qtype(dict_qtype)), value_qtype
      )

  def test_not_enough_info_to_infer_attr(self):
    self.assertIsNone(M.qtype.make_dict_qtype(L.x, arolla.FLOAT32).qtype)
    self.assertIsNone(M.qtype.make_dict_qtype(arolla.INT32, L.x).qtype)

    no_key_qvalue = M.qtype.make_dict_qtype(
        M.annotation.qtype(L.x, arolla.QTYPE), arolla.INT32
    )
    arolla.testing.assert_qvalue_allequal(no_key_qvalue.qtype, arolla.QTYPE)
    self.assertIsNone(no_key_qvalue.qvalue)

    no_value_qvalue = M.qtype.make_dict_qtype(
        arolla.INT32, M.annotation.qtype(L.x, arolla.QTYPE)
    )
    arolla.testing.assert_qvalue_allequal(no_value_qvalue.qtype, arolla.QTYPE)
    self.assertIsNone(no_value_qvalue.qvalue)

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected QTYPE, got key_qtype: INT32'),
    ):
      _ = M.qtype.make_dict_qtype(1, L.x)

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected QTYPE, got value_qtype: FLOAT32'),
    ):
      _ = M.qtype.make_dict_qtype(L.x, 2.5)


if __name__ == '__main__':
  absltest.main()
