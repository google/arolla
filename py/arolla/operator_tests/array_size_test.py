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

"""Tests for M.array.size."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils

M = arolla.M


class ArraySize(parameterized.TestCase):

  @parameterized.named_parameters(
      utils.product_parameters(
          utils.CONST_ARRAY_FACTORIES,
          utils.SCALAR_VALUES + utils.OPTIONAL_VALUES,
          utils.SIZES,
      )
  )
  def test(self, gen_const_array, value, size):
    self.assertEqual(
        arolla.eval(M.array.size(gen_const_array(value, size))), size
    )


if __name__ == '__main__':
  absltest.main()
