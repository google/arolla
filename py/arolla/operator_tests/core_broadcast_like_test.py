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

"""Tests for M.core.broadcast_like."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils

M = arolla.M

ARRAY_FACTORY_VALUE_SIZES = tuple(
    utils.product_parameters(
        utils.CONST_ARRAY_FACTORIES,
        utils.OPTIONAL_VALUES,
        utils.SIZES,
    )
)


class CoreBroadcastLike(parameterized.TestCase):

  @parameterized.named_parameters(
      utils.product_parameters(utils.SCALAR_VALUES, utils.SCALAR_VALUES)
  )
  def testScalarToScalar(self, target_value, value):
    actual_value_fn = M.core.broadcast_like(target_value, value)
    actual_value = arolla.eval(actual_value_fn)
    arolla.testing.assert_qvalue_allequal(actual_value, value)

  @parameterized.named_parameters(
      utils.product_parameters(utils.OPTIONAL_VALUES, utils.SCALAR_VALUES)
  )
  def testScalarToOptional(self, target_value, value):
    actual_value_fn = M.core.broadcast_like(target_value, value)
    actual_value = arolla.eval(actual_value_fn)
    arolla.testing.assert_qvalue_allequal(
        actual_value, value & arolla.present()
    )

  @parameterized.named_parameters(
      utils.product_parameters(utils.OPTIONAL_VALUES, utils.OPTIONAL_VALUES)
  )
  def testOptionalToOptional(self, target_value, value):
    actual_value_fn = M.core.broadcast_like(target_value, value)
    actual_value = arolla.eval(actual_value_fn)
    arolla.testing.assert_qvalue_allequal(actual_value, value)

  @parameterized.named_parameters(
      utils.product_parameters(
          ARRAY_FACTORY_VALUE_SIZES, utils.SCALAR_VALUES + utils.OPTIONAL_VALUES
      )
  )
  def testValueToArray(
      self,
      target_array_factory_fn,
      target_array_value,
      target_array_size,
      value,
  ):
    target_array_fn = target_array_factory_fn(
        target_array_value, target_array_size
    )
    actual_value_fn = M.core.broadcast_like(target_array_fn, value)
    self.assertEqual(
        utils.pyval(actual_value_fn), [utils.pyval(value)] * target_array_size
    )


if __name__ == '__main__':
  absltest.main()
