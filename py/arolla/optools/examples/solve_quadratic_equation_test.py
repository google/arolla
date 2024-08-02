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

"""Testing of arolla_operator_package rule usage."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.optools.examples import solve_quadratic_equation_clib as _


class Test(parameterized.TestCase):

  @parameterized.parameters(
      (1.0, 2.0, 1.0, -1.0, -1.0),
      (1.0, -2.0, 1.0, 1.0, 1.0),
      (1.0, 0.0, -1.0, -1.0, 1.0),
      (-3.0, 2.0, 1.0, -1.0 / 3, 1.0),
  )
  def test(self, a, b, c, r0, r1):
    x0, x1 = arolla.eval(
        arolla.M.experimental.solve_quadratic_equation(a, b, c)
    )
    self.assertAlmostEqual(x0, r0)
    self.assertAlmostEqual(x1, r1)


if __name__ == "__main__":
  absltest.main()
