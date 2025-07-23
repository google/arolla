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

"""Tests for M.qtype.get_field_qtypes operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = (
    (arolla.QTYPE, arolla.types.make_sequence_qtype(arolla.QTYPE)),
)


class QTypeGetFieldQTypesTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.get_field_qtypes, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      (arolla.INT32, arolla.types.Sequence(value_qtype=arolla.QTYPE)),
      (arolla.OPTIONAL_UNIT, arolla.types.Sequence(arolla.BOOLEAN)),
      (
          arolla.OPTIONAL_INT32,
          arolla.types.Sequence(arolla.BOOLEAN, arolla.INT32),
      ),
      (
          arolla.types.make_tuple_qtype(),
          arolla.types.Sequence(value_qtype=arolla.QTYPE),
      ),
      (
          arolla.types.make_tuple_qtype(arolla.INT32, arolla.INT64),
          arolla.types.Sequence(arolla.INT32, arolla.INT64),
      ),
  )
  def test_eval(self, arg_qvalue, expected_output_qvalue):
    actual_output_qvalue = arolla.eval(M.qtype.get_field_qtypes(arg_qvalue))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )


if __name__ == '__main__':
  absltest.main()
