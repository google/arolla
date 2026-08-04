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

"""Expression to demonstrate batch scalar mix expression.

The code is here to demonstrate ability to get the best of the batch and scalar
worlds including avoiding pivoting (converting input data into arrays).
"""

from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


def floats(*exprs):
  return (M.annotation.qtype(e, arolla.FLOAT32) for e in exprs)


def array_floats(*exprs):
  return (M.annotation.qtype(e, arolla.DENSE_ARRAY_FLOAT32) for e in exprs)


def x_expr(floats_fn) -> arolla.Expr:
  a, b, c, d, e = floats_fn(L.a, L.b, L.c, L.d, L.e)
  return (a + b * e - a * d) * (c * a / b - d * (e / b) * a) / e / b


def y_expr(floats_fn) -> arolla.Expr:
  q, w, e, r, t = floats_fn(L.q, L.w, L.e, L.r, L.t)
  return (q * t / w - 2.0 * w / r) / (e * t / q + (r / e) * (r / t) / 2.0) * t


def batch_expr(x: arolla.Expr, y: arolla.Expr) -> arolla.Expr:
  """Returns expression to be computed on batches."""
  return M.math.sum(x) * 4.0 + M.math.sum(y) * 3.0


def fully_batch_expr() -> arolla.Expr:
  """Returns expression fully based on batch evaluation with pivoting."""
  return batch_expr(x_expr(array_floats), y_expr(array_floats))


def hybrid_batch_expr() -> arolla.Expr:
  """Returns batch part of hybrid expression based on cached x and y."""
  x, y = array_floats(L.x, L.y)
  return batch_expr(x, y)


def hybrid_pointwise_expr() -> arolla.Expr:
  """Returns pointwise part of hybrid expression exporting x and y."""
  x = M.annotation.export(x_expr(floats), 'x')
  y = M.annotation.export(y_expr(floats), 'y')
  first_op = arolla.LambdaOperator('r, *_args', P.r, name='first_no_optimize')
  return first_op(arolla.present(), x, y)
