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

"""Collection of test expressions on batches."""

from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


def floats(*exprs):
  return (M.annotation.qtype(e, arolla.DENSE_ARRAY_FLOAT32) for e in exprs)


def x_plus_y_times_5():
  x, y = floats(L.x, L.y)
  return (x + y) * 5.0


def two_long_fibonacci_chains():
  """Test case to demonstrate lambdas generation. Result is always 0."""
  lx, ly = floats(L.x, L.y)
  x = lx + ly
  y = -lx - ly
  a = x + y  # = 0
  b = y + x  # = 0
  for _ in range(10):
    # a + b is always equal to 0
    x, a = a, a + x
    y, b = b, b + y
  return a + b


def aggregation_dot_product_times_5():
  """Test case to demonstrate aggregation. Result is 5*dot_product(x,y)."""
  lx, ly = floats(L.x, L.y)
  return 5.0 * M.math.sum(lx * ly)


# Benchmarks


def float_benchmark_expr(op_count: str):
  """~op_count operations on floats. Operations depend on previous step."""
  x, y = floats(L.x, L.y)
  z = x + y
  for i in range(int(op_count)):
    if i % 2 == 0:
      z += x
    else:
      z += y
  return z
