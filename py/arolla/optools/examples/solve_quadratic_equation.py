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

"""Operator declaration: M.experimental.solve_quadratic_equation."""

from arolla import arolla


@arolla.optools.add_to_registry('experimental.solve_quadratic_equation')
@arolla.optools.as_lambda_operator('solve_quadratic_equation')
def solve_quadratic_equation(a, b, c):
  b = b / a
  c = c / a
  d = (b * b - 4 * c) ** 0.5
  x0 = (-b - d) / 2
  x1 = (-b + d) / 2
  return arolla.M.core.make_tuple(x0, x1)
