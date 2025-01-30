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

"""Benchmarks for arolla.abc.

IMPORTANT: These benchmarks are highly dependent on the compilation cache and do
not include the time taken for initial compilation.
"""

from arolla import arolla
import google_benchmark

L = arolla.L
M = arolla.M


@google_benchmark.register
def invoke_op_add_scalar(state):
  one = arolla.int32(1)
  math_add = M.math.add

  # Warm up compilation cache.
  arolla.abc.invoke_op(math_add, (one, one))

  while state:
    arolla.abc.invoke_op(math_add, (one, one))


@google_benchmark.register
def invoke_op_by_name_add_scalar(state):
  one = arolla.int32(1)

  # Warm up compilation cache.
  arolla.abc.invoke_op('math.add', (one, one))

  while state:
    arolla.abc.invoke_op('math.add', (one, one))


@google_benchmark.register
def eval_expr_add_scalar(state):
  x = arolla.int32(1)
  expr = L.x + L.x

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, x=x)

  while state:
    arolla.abc.eval_expr(expr, x=x)


@google_benchmark.register
def eval_expr_add_scalar_x1000(state):
  x = arolla.int32(1)
  expr = L.x
  for _ in range(10**3):
    expr += L.x

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, x=x)

  while state:
    arolla.abc.eval_expr(expr, x=x)


@google_benchmark.register
def eval_expr_add_array_n10(state):
  x = arolla.array_int32([1] * 10)
  expr = L.x + L.x

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, x=x)

  while state:
    arolla.abc.eval_expr(expr, x=x)


@google_benchmark.register
def eval_expr_add_array_n10_x1000(state):
  x = arolla.array_int32([1] * 10)
  expr = L.x
  for _ in range(10**3):
    expr += L.x

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, x=x)

  while state:
    arolla.abc.eval_expr(expr, x=x)


@google_benchmark.register
def eval_expr_add_array_n1000(state):
  x = arolla.array_int32([1] * 1000)
  expr = L.x + L.x

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, x=x)

  while state:
    arolla.abc.eval_expr(expr, x=x)


@google_benchmark.register
def eval_expr_add_array_n1000_x1000(state):
  x = arolla.array_int32([1] * 1000)
  expr = L.x
  for _ in range(10**3):
    expr += L.x

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, x=x)

  while state:
    arolla.abc.eval_expr(expr, x=x)


@google_benchmark.register
def eval_expr_dense_rank_array_n10(state):
  n = arolla.int64(10)
  expr = M.array.dense_rank(
      M.array.randint_with_shape(M.array.make_array_shape(L.n))
  )

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, n=n)

  while state:
    arolla.abc.eval_expr(expr, n=n)


@google_benchmark.register
def eval_expr_dense_rank_array_n1000_edge_to_scalar(state):
  n = arolla.int64(1000)
  expr = M.array.dense_rank(
      M.array.randint_with_shape(M.array.make_array_shape(L.n))
  )

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, n=n)

  while state:
    arolla.abc.eval_expr(expr, n=n)


@google_benchmark.register
def eval_expr_dense_rank_array_n1000_edge_from_sizes(state):
  n = arolla.int64(1000)
  x = M.array.randint_with_shape(M.array.make_array_shape(L.n), seed=1)
  e = M.edge.from_sizes(
      M.core.const_with_shape(M.array.make_array_shape(L.n // 50), 50)
  )
  expr = M.array.dense_rank(x, e)

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, n=n)

  while state:
    arolla.abc.eval_expr(expr, n=n)


@google_benchmark.register
def eval_expr_dense_rank_array_n1000_edge_from_mapping(state):
  n = arolla.int64(1000)
  x = M.array.randint_with_shape(M.array.make_array_shape(L.n), seed=1)
  e = M.edge.from_mapping(
      M.array.randint_with_shape(
          M.array.make_array_shape(L.n), low=0, high=100, seed=2
      ),
      100,
  )
  expr = M.array.dense_rank(x, e)

  # Warm up compilation cache.
  arolla.abc.eval_expr(expr, n=n)

  while state:
    arolla.abc.eval_expr(expr, n=n)


if __name__ == '__main__':
  google_benchmark.main()
