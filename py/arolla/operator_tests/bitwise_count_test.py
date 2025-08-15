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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  """Returns supported qtype signatures for bitwise.count.

  Returns: tuple(
      (arg_qtype, result_qtype),
      ...
  )
  """
  return pointwise_test_utils.lift_qtypes(
      (arolla.types.INT64, arolla.types.INT32),
      (arolla.types.INT32, arolla.types.INT32),
  )


QTYPE_SIGNATURES = gen_qtype_signatures()


class BitwiseCountTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.bitwise.count, QTYPE_SIGNATURES)

  @parameterized.parameters(
      *((arolla.int32(x), arolla.int32(x.bit_count())) for x in range(8)),
      *((arolla.int64(x), arolla.int32(x.bit_count())) for x in range(8)),
      # Python does not use two's complement to represent negative integers.
      *(
          (arolla.int32(x), 32 - (x + 1).bit_count())
          for x in range(-8)
      ),
      *(
          (arolla.int64(x), 64 - (x + 1).bit_count())
          for x in range(-8)
      ),
  )
  def test_eval(self, arg, expected_value):
    actual_value = self.eval(M.bitwise.count(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
