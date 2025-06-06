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

"""Collection of test expressions on scalars."""

import functools

from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


def floats(*exprs):
  return (M.annotation.qtype(e, arolla.FLOAT32) for e in exprs)


def optional_floats(*exprs):
  return (M.annotation.qtype(e, arolla.OPTIONAL_FLOAT32) for e in exprs)


def doubles(*exprs):
  return (M.annotation.qtype(e, arolla.FLOAT64) for e in exprs)


def texts(*exprs):
  return (M.annotation.qtype(e, arolla.TEXT) for e in exprs)


def text_contains():
  text, substr = texts(L.text, L.substr)
  return M.strings.contains(text, substr)


def bytes_contains_me():
  return M.strings.contains(
      M.annotation.qtype(L.text, arolla.BYTES), "me→".encode("utf-8")
  )


def variadic_hello_str_join():
  return M.strings.join(
      "Hello, ",
      M.annotation.qtype(L.title, arolla.TEXT),
      " ",
      M.annotation.qtype(L.name, arolla.TEXT),
      "!",
  )


def variadic_hello_str_join_optional():
  return M.strings.join(
      b"Hello, ",
      M.annotation.qtype(L.title, arolla.OPTIONAL_BYTES),
      b" ",
      M.annotation.qtype(L.name, arolla.OPTIONAL_BYTES),
      b"!",
  )


def variadic_equation_str_printf():
  a = M.annotation.qtype(L.a, arolla.INT32)
  b = M.annotation.qtype(L.b, arolla.INT32)
  return M.strings.printf(b"%d + %d = %d", a, b, a + b)


def variadic_equation_str_printf_optional():
  a = M.annotation.qtype(L.a, arolla.INT32)
  b = M.annotation.qtype(L.b, arolla.OPTIONAL_INT32)
  return M.strings.printf(b"%d + %d = %d", a, b, a + b)


def literal_one():
  return arolla.literal(1.0)


def identity_x():
  return M.annotation.qtype(L.x, arolla.FLOAT32)


def x_plus_y_times_5():
  x, y = doubles(L.x, L.y)
  return (x + y) * 5.0


def status_or_test_zero_result():
  """Test for the expression with operators returning StatusOr.

  Test supposed to contain varaity of constructions:
  * Local variables
  * Inlined calls
  * Lambdas
  * Functions/Lambdas with final call returning not StatusOr.
  * Export of result with failure
  * Unused in the final result export with failure

  Returns:
    Test expression:
    * final result is always zero
    * x_floordiv_y named output equal to x // y
    * y_floordiv_x named output equal to y // x
  """
  x, y = floats(L.x, L.y)
  y_floordiv_x = M.math.floordiv(y, x)
  z = M.annotation.export(M.math.floordiv(x, y), "x_floordiv_y")
  null_z = z - z
  y_floordiv_x_export = M.annotation.export(
      y_floordiv_x + null_z, "y_floordiv_x"
  )

  mz = -z
  nf = z + mz + null_z
  z2 = z * z + y_floordiv_x
  mz2 = -z2
  sqf = z2 + mz2
  first_op = arolla.LambdaOperator("x, _y", P.x, name="first_no_optimize")

  # testing the function that return StatusOr, but with a previous assignments.
  q = M.math.floordiv(x + x, y + y)
  q_zero = q * 2.0 - q * 3.0 + q

  return first_op(nf + sqf, y_floordiv_x_export) + q_zero


def conditional_operators_test_zero_result():
  """Test for the expression with operators that contain branches.

  Test supposed to contain varaity of constructions:
  * Passing local variables and literals
  * Inlined calls
  * Passing status
  * full optimizations
  * variables used only in second arg from `|` operation
  * export of conditionally computed variables

  Returns:
    Test expression:
    * final result is always zero
    * if y == 0, division by zero returned
    * 'null' named output always equal to 0.
      The result is not used for final evaluation if x < 0.
  """
  x, y = optional_floats(L.x, L.y)

  def full_null(z, wrap_conditional=lambda x: x):
    mz = -z
    null1 = z + mz
    q = z + null1
    mq = -q
    null2 = q + mq
    return (null1 & (x < 0.0)) | wrap_conditional(null2) | 0.0

  full_null_status = full_null(M.math.floordiv(x, y))
  full_null2 = full_null(M.core.where(x < y, x - y, x + y))
  full_null3 = full_null(full_null2)
  full_null4 = full_null(
      (x | y) & (M.core.has(x) & M.core.has(y)),
      wrap_conditional=lambda x: M.annotation.export(x, "null"),
  )
  full_null5 = full_null(
      M.bool.logical_if(M.bool.less(x, y), x * y, x**y, x / y)
  )
  return full_null3 * full_null2 + full_null4 + full_null_status + full_null5


def const_ref_returning_operator_x_plus_y_result():
  """Test with operator that return const& to test correct lifetime."""
  x, y, z = optional_floats(L.x, L.y, L.z)
  x_or0 = x | 0.0
  y_or0 = y | 0.0
  z_or0 = z | 0.0
  union_plus = 2.0 * x_or0 + y_or0 - x_or0
  if_union_plus = M.bool.logical_if(
      M.bool.less(x, y), union_plus, union_plus, union_plus + 1.0
  )
  return if_union_plus - 2.0 * (union_plus + 1.0) + z_or0


def x_plus_y_times_32_with_named_nodes():
  x, y = doubles(L.x, L.y)
  res = x + y
  for i in range(3):
    res = res + M.annotation.name(res, str(i))
  two = arolla.literal(arolla.float64(2.0))
  return res * two * M.annotation.name(two, "a")


def x_plus_y_times_5_with_export():
  x, y = doubles(L.x, L.y)
  return (
      M.annotation.export(M.annotation.export_value(x + y, "xty", x * y), "xpy")
      * 5.0
  )


def x_plus_y_times_5_with_unused_export_x_minus_5():
  """Test expression for exporting value that is not used for final output."""
  first_op = arolla.LambdaOperator("x, _y", P.x, name="first_no_optimize")
  x, y = doubles(L.x, L.y)
  return first_op(
      M.annotation.name(M.annotation.export(x + y, "xpy"), "xpy") * 5.0,
      M.annotation.name(M.annotation.export(x - 5.0, "xm5"), "xm5"),
  )


def x_plus_y_times_5_with_unused_two_nested_exports_xm5_andxm10():
  """Exported value is used by other exported value, but not final output."""
  first_op = arolla.LambdaOperator("x, _y, _z", P.x, name="first_no_optimize")
  x, y = doubles(L.x, L.y)
  xm5 = M.annotation.name(M.annotation.export(x - 5.0, "xm5"), "xm5")
  xm10 = M.annotation.name(M.annotation.export(xm5 - 5.0, "xm10"), "xm10")
  return first_op(
      M.annotation.name(M.annotation.export(x + y, "xpy"), "xpy") * 5.0,
      xm5,
      xm10,
  )


def x_plus_y_times_5_nested_export():
  """Test expression for nesting export values."""
  x, y = doubles(L.x, L.y)
  return (
      M.annotation.export(
          M.annotation.export(x, "x") + M.annotation.export(y, "y"), "xpy"
      )
      * 5.0
  )


def x_plus_y_times_5_duplicated_export():
  """Test expression for exporting the same value with different names."""
  x, y = doubles(L.x, L.y)
  return (
      M.annotation.export(M.annotation.export(x, "x"), "x2")
      + M.annotation.export(x, "x3")
      - x
      + y
  ) * 5.0


def x_plus_y_times_5_duplicated_export_unused():
  """Test expression for exporting unused value with two different names."""
  first_op = arolla.LambdaOperator("x, _y", P.x, name="first_no_optimize")
  x, y = doubles(L.x, L.y)
  return first_op(
      (x + y) * 5.0,
      M.annotation.export(M.annotation.export(x * y, "xy"), "xy2"),
  )


def x_plus_y_times_x():
  x, y = floats(L.x, L.y)
  return (x + y) * x


def x_plus_y_optional():
  x, y = optional_floats(L.x, L.y)
  return x + y


def two_independent_fns():
  x, y = floats(L.x, L.y)
  return (x * y) + (x + y)


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


def big_inline_chain():
  """Test case to demonstrate inlining. Result is always 0."""
  lx, ly = floats(L.x, L.y)
  x = lx + ly
  y = -lx - ly
  return functools.reduce(lambda x, y: x + y, [x, y] * 5, x + y)


def many_nested_long_fibonacci_chains():
  """Test case to make sure that we do not go over limit inlining lambdas."""
  lx, ly = floats(L.x, L.y)
  x = lx + ly
  y = -lx - ly
  sa = x + y  # = 0
  sb = sa + sa  # = 0
  a, b = sa, sb
  for _ in range(30):
    # fibonacci chain, which could be place to the lambda
    for _ in range(50):
      # a and b are always equal to 0
      a, b = b, a + b
    # many inlined nodes
    a = functools.reduce(lambda x, y: x + y, [sa] * 50, b) + sa
    b = a + a + sb
  return functools.reduce(
      lambda x, y: x + y, [b] + [sa] * 50, a
  )  # many inlined nodes


def many_inputs_and_side_outputs(n: str):
  """n inputs and side outputs."""
  n = int(n)
  inputs = floats(*[L[f"input_{i}"] for i in range(n)])
  outputs = [
      M.annotation.export(x**2.0, f"output_{i}") for i, x in enumerate(inputs)
  ]
  return sum(outputs[1:], outputs[0])


def derived_qtype_casts():
  """Test case for derived_qtype.[downcast,upcast] operators."""
  x = M.annotation.qtype(L.x, arolla.FLOAT64)
  wx = M.derived_qtype.downcast(arolla.WEAK_FLOAT, x)
  return M.derived_qtype.upcast(arolla.WEAK_FLOAT, wx)


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


def optional_float_benchmark_expr(op_count: str):
  """~op_count operations on optionals. Operations depend on previous step."""
  x, y = optional_floats(L.x, L.y)
  z = x + y
  for i in range(int(op_count)):
    if i % 2 == 0:
      z += x
    else:
      z += y
  return z


def double_benchmark_expr(op_count: str):
  """~op_count operations on doubles. Each operation depend on previous step."""
  x, y = doubles(L.x, L.y)
  z = x + y
  for i in range(int(op_count)):
    if i % 2 == 0:
      z += x
    else:
      z += y
  return z


def double_two_parallel_computes_benchmark_expr(op_count):
  """~op_count operations. Two operations on every step are independent."""
  x, y = doubles(L.x, L.y)
  for _ in range(int(op_count) // 2):
    x, y = (x - y, y - x)
  return x + y  # the sum is always zero
