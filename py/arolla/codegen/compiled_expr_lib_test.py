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

"""Tests for CompiledExprLib python utilities."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.codegen import compiled_expr_lib

M = arolla.M
L = arolla.L


class CompiledExprLibTest(parameterized.TestCase):

  @parameterized.parameters([
      (L.x, 'L.x'),
      (L.x + L.y, 'L.x + L.y'),
      (
          M.core.all(arolla.present(), arolla.types.ScalarToScalarEdge()),
          'M.core.all(present, <value of SCALAR_TO_SCALAR_EDGE>)',
      ),
  ])
  def test_get_expr_text(self, expr, expected_str):
    self.assertEqual(compiled_expr_lib.get_expr_text(expr), expected_str)


if __name__ == '__main__':
  absltest.main()
