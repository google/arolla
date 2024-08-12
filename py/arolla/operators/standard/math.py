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

"""Declaration of M.math.* operators."""

from arolla import arolla
from arolla.operators.standard import array as M_array
from arolla.operators.standard import core as M_core
from arolla.operators.standard import edge as M_edge
from arolla.operators.standard import qtype as M_qtype

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


subtract = M_array._subtract  # pylint: disable=protected-access


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.float',
    qtype_constraints=[constraints.expect_numerics(P.x)],
)
def float_(x):
  """Converts `x` to floating-point number, if possible.

  If `x` is a floating-point number, the operator returns it unchanged.
  Otherwise, it casts `x` to float32.

  Args:
    x: A numeric value.

  Returns:
    A floating-point value.
  """
  nop_case = arolla.types.RestrictedLambdaOperator(
      P.x, qtype_constraints=[constraints.expect_floats(P.x)]
  )
  return arolla.optools.dispatch[nop_case, M_core.to_float32](x)


_unary_numeric_predicate = dict(
    qtype_constraints=[constraints.expect_numerics(P.x)],
    qtype_inference_expr=M_qtype.get_presence_qtype(P.x),
)


# TODO: Optimize to "always true" for int inputs.
@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.is_finite', **_unary_numeric_predicate
)
def is_finite(x):
  """Returns a present value iff x is a finite value, i.e. not +/-INF or NaN."""
  raise NotImplementedError('provided by backend')


# TODO: Optimize to "always false" for int inputs.
@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.is_inf', **_unary_numeric_predicate)
def is_inf(x):
  """Returns a present value iff x is a positive or negative infinity.."""
  raise NotImplementedError('provided by backend')


# TODO: Optimize to "always false" for int inputs.
@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.is_nan', **_unary_numeric_predicate)
def is_nan(x):
  """Returns a present value iff x is a not-a-number (NaN) value."""
  raise NotImplementedError('provided by backend')


# TODO: Refactor to _numeric_to_common_type(*args).
_unary_numeric = dict(
    qtype_constraints=[constraints.expect_numerics(P.x)],
    qtype_inference_expr=P.x,
)

_binary_numeric = dict(
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.y),
        constraints.expect_implicit_cast_compatible(P.x, P.y),
    ],
    qtype_inference_expr=M_qtype.common_qtype(P.x, P.y),
)

# TODO: Refactor to _numeric_to_common_float_type(*args).
_unary_numeric_to_float = dict(
    qtype_constraints=[constraints.expect_numerics(P.x)],
    qtype_inference_expr=M_qtype.get_float_qtype(P.x),
)

_binary_numeric_to_common_float = dict(
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.y),
        constraints.expect_implicit_cast_compatible(P.x, P.y),
    ],
    qtype_inference_expr=constraints.common_float_qtype_expr(P.x, P.y),
)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.abs', **_unary_numeric)
def abs_(x):
  """Returns numerical absolute value element-wise."""
  raise NotImplementedError('provided by backend')


abs = abs_  # pylint: disable=redefined-builtin


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._weighted_cdf',
    qtype_inference_expr=M.qtype.with_value_qtype(
        M_qtype.get_shape_qtype(P.x),
        M.qtype.conditional_qtype(
            M_qtype.get_value_qtype(P.x) == arolla.FLOAT64,
            arolla.FLOAT64,
            arolla.FLOAT32,
        ),
    ),
)
def _weighted_cdf(x, weights, over):
  """Backend proxy without default arg handling."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.cdf',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        (
            (
                M.qtype.is_numeric_qtype(P.weights)
                & (
                    M.qtype.get_shape_qtype(P.weights)
                    == M.qtype.get_shape_qtype(P.x)
                )
            )
            | (P.weights == arolla.UNSPECIFIED),
            (
                '`weights` must be numeric and have the same shape as `x` or'
                ' be left unspecified, got'
                f' {constraints.name_type_msg(P.weights)},'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
        *constraints.expect_edge_or_unspecified(P.over, child_side_param=P.x),
    ],
)
def cdf(x, weights=arolla.unspecified(), over=arolla.unspecified()):
  """Returns the CDF of x element-wise.

  The CDF is an array of floating-point values of the same shape
  where each element represents which percentile the corresponding element in x
  is situated at in its sorted group, i.e. the percentage of values in the group
  that are smaller than or equal to it.

  Args:
    x: An array of numbers.
    weights: If provided, will compute weighted CDF: each output value will
      correspond to the weight percentage of values smaller than or equal to x.
    over: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  over = M_core.default_if_unspecified(over, M_edge.to_scalar(x))
  weights = M.core.default_if_unspecified(weights, M.core.ones_like(x))
  return _weighted_cdf(x, weights, over)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.neg', **_unary_numeric)
def neg(x):
  """Returns numerical negative value element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.pos', qtype_constraints=[constraints.expect_numerics(P.x)]
)
def pos(x):
  """Returns unchanged numerical value element-wise."""
  return x


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.sign', **_unary_numeric)
def sign(x):
  """Returns the sign (+/-1) element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._round',
    qtype_constraints=[constraints.expect_floats(P.x)],
    qtype_inference_expr=P.x,
)
def _round(x):
  """(internal) Rounds the values to the nearest integer, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.round', qtype_constraints=(constraints.expect_numerics(P.x),)
)
def round_(x):
  """Rounds the values to the nearest integer, element-wise."""
  return arolla.optools.dispatch[_round, M_core.identity](x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._floor',
    qtype_constraints=[constraints.expect_floats(P.x)],
    qtype_inference_expr=P.x,
)
def _floor(x):
  """(internal) Returns the floor of the input, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.floor', qtype_constraints=(constraints.expect_numerics(P.x),)
)
def floor(x):
  """Return the floor of the input, element-wise."""
  return arolla.optools.dispatch[_floor, M_core.identity](x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._ceil',
    qtype_constraints=[constraints.expect_floats(P.x)],
    qtype_inference_expr=P.x,
)
def _ceil(x):
  """(internal) Returns the ceiling of the input, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.ceil', qtype_constraints=[constraints.expect_numerics(P.x)]
)
def ceil(x):
  """Return the ceiling of the input, element-wise."""
  return arolla.optools.dispatch[_ceil, M_core.identity](x)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.log', **_unary_numeric_to_float)
def log(x):
  """Returns natural logarithm of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.log1p', **_unary_numeric_to_float)
def log1p(x):
  """Returns natural logarithm of 1 + x element-wise.."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.log2', **_unary_numeric_to_float)
def log2(x):
  """Returns base-2 logarithm of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.log10', **_unary_numeric_to_float)
def log10(x):
  """Returns base-10 logarithm of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.log_sigmoid', **_unary_numeric_to_float
)
def log_sigmoid(x):
  """Returns natural logarithm of sigmoid of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.logit', **_unary_numeric_to_float)
def logit(x):
  """Returns log(1 / (1 - x)) element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.symlog1p', **_unary_numeric_to_float)
def symlog1p(x):
  """Returns log1p(|x|) * sign(x) element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.exp', **_unary_numeric_to_float)
def exp(x):
  """Returns exponential of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.expm1', **_unary_numeric_to_float)
def expm1(x):
  """Returns exp(x) - 1 element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.trig.atan', **_unary_numeric_to_float)
def trig_atan(x):
  """Returns the trignometric inverse tangent of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.trig.cos', **_unary_numeric_to_float)
def trig_cos(x):
  """Returns the cosine of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.trig.sin', **_unary_numeric_to_float)
def trig_sin(x):
  """Returns the sine of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.trig.sinh', **_unary_numeric_to_float)
def trig_sinh(x):
  """Returns the hyperbolic sine of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.add', **_binary_numeric)
def add(x, y):
  """Returns x + y element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.multiply', **_binary_numeric)
def multiply(x, y):
  """Returns x * y element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.floordiv', **_binary_numeric)
def floordiv(x, y):
  """Divides x / y element-wise, rounding toward -inf."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.mod', **_binary_numeric)
def mod(x, y):
  """Returns the remainder of floordiv operation element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.maximum', **_binary_numeric)
def maximum(x, y):
  """Returns the maximum of x and y element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('math.minimum', **_binary_numeric)
def minimum(x, y):
  """Returns the minimum of x and y element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.divide', **_binary_numeric_to_common_float
)
def divide(x, y):
  """Divides x / y element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.fmod', **_binary_numeric_to_common_float
)
def fmod(x, y):
  """Returns the remainder of division element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._pow', **_binary_numeric_to_common_float
)
def _pow(x, y):
  """(internal) Returns x raised to power y, element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('math.pow')
def pow_(x, y):
  """Returns x raised to power y, element-wise."""
  return _pow(x, float_(y))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.is_close',
    qtype_constraints=[
        # Restricting the inputs to floats because implicit casting to float32
        # is lossy and might be error prone for this operator.
        constraints.expect_floats(P.x),
        constraints.expect_floats(P.y),
        (
            M_qtype.is_numeric_qtype(P.rtol) | (P.rtol == arolla.UNSPECIFIED),
            (
                'expected floating-points or unspecified, got'
                f' {constraints.name_type_msg(P.rtol)}'
            ),
        ),
        constraints.expect_numerics(P.atol),
        (
            constraints.broadcast_qtype_expr(
                [P.x, P.y, P.atol],
                M_qtype.conditional_qtype(
                    P.rtol != arolla.UNSPECIFIED, P.rtol, arolla.UNIT
                ),
            )
            != arolla.NOTHING,
            (
                'broadcast incompatible types'
                f' {constraints.name_type_msg(P.x)},'
                f' {constraints.name_type_msg(P.y)},'
                f' {constraints.name_type_msg(P.rtol)},'
                f' {constraints.name_type_msg(P.atol)}'
            ),
        ),
    ],
)
def is_close(x, y, rtol=arolla.unspecified(), atol=arolla.float32(0.0)):
  """Returns a present value iff x is close to y.

  Args:
    x: A number to compare.
    y: A number to compare.
    rtol: Relative tolerance, the maximum allowed difference between 'x' and
      'y', as a fraction of the larger of |x| and |y|. The default values are
      1e-6 for float32 arguments and 1e-9 for float64.
    atol: Absolute tolerance, the maximum allowed difference between 'x' and
      'y', which is useful for comparisons near zero. 'atol' must always be
      nonnegative.

  Returns:
    a present value iff x is close to y.
  """
  args_scalar_qtype = constraints.common_float_qtype_expr(
      M_qtype.scalar_qtype_of(x), M_qtype.scalar_qtype_of(y)
  )
  rtol_or_default = M_core.default_if_unspecified(
      rtol,
      (arolla.float32(1e-6) & (args_scalar_qtype == arolla.FLOAT32))
      | arolla.float64(1e-9),
  )
  return (x == y) | (
      is_finite(x)
      & is_finite(y)
      & (
          abs_(x - y)
          <= maximum(rtol_or_default * maximum(abs_(x), abs_(y)), atol)
      )
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._searchsorted_impl',
    qtype_constraints=[
        constraints.expect_dense_array(P.haystack),
        constraints.expect_numerics(P.haystack),
        (
            M.qtype.common_qtype(
                M.qtype.get_scalar_qtype(P.haystack),
                M.qtype.get_scalar_qtype(P.needle),
            )
            != arolla.NOTHING,
            (
                'arguments do not have a common scalar qtype:'
                f' {constraints.name_type_msg(P.haystack)} and'
                f' {constraints.name_type_msg(P.needle)}'
            ),
        ),
        constraints.expect_boolean(P.right),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.needle, arolla.INT64),
)
def _searchsorted_impl(haystack, needle, right):
  """Returns an index where an element needs be inserted to maintain order."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.searchsorted',
    qtype_constraints=[constraints.expect_array(P.haystack)],
)
def searchsorted(haystack, needle, right=False):
  """Returns an index where an element needs be inserted to maintain order."""
  return _searchsorted_impl(M_array.as_dense_array(haystack), needle, right)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._inverse_cdf',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), M_qtype.get_scalar_qtype(P.x)
    ),
)
def _inverse_cdf(x, into, cdf_arg):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.inverse_cdf',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        constraints.expect_numerics(P.cdf_arg),
        constraints.expect_scalar_or_optional(P.cdf_arg),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def inverse_cdf(x, cdf_arg, into=arolla.unspecified()):
  """Returns the value with CDF (in [0, 1]) approximately equal to the input.

  The return value will have an offset of floor((cdf - 1e-6) * size()) in the
  (ascendingly) sorted array.

  Args:
    x: (array) An array of numbers.
    cdf_arg: (float) CDF value.
    into: (Edge) An edge to aggregate into. If not specified, the entire array
      is considered a single group.

  Returns:
    An array (or optional value, if aggregating to scalar) of values of
    the same type as `x`.
  """
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  return _inverse_cdf(x, into, M_core.to_float32(cdf_arg))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.t_distribution_inverse_cdf',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.degrees_of_freedom),
        constraints.expect_implicit_cast_compatible(P.x, P.degrees_of_freedom),
    ],
    qtype_inference_expr=constraints.common_float_qtype_expr(
        P.x, P.degrees_of_freedom
    ),
)
def t_distribution_inverse_cdf(x, degrees_of_freedom):
  """Student's t-distribution inverse CDF."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.beta_distribution_inverse_cdf',
    qtype_constraints=[
        constraints.expect_numerics(P.cdf_arg),
        constraints.expect_numerics(P.alpha),
        constraints.expect_numerics(P.beta),
        constraints.expect_implicit_cast_compatible(P.cdf_arg, P.alpha, P.beta),
    ],
    qtype_inference_expr=constraints.common_float_qtype_expr(
        P.cdf_arg, P.alpha, P.beta
    ),
)
def beta_distribution_inverse_cdf(cdf_arg, alpha, beta):
  """Returns the Beta distribution inverse CDF.

  See https://en.wikipedia.org/wiki/Beta_distribution for the definition.

  Args:
    cdf_arg: The probability for which to compute the inverse CDF.
    alpha: The alpha parameter of the distribution.
    beta: The beta parameter of the distribution.

  Returns:
    The value v such that the probability that the Beta distribution with
    the given parameters is less than or equal to v is equal to x.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.normal_distribution_inverse_cdf',
    **_unary_numeric_to_float,
)
def normal_distribution_inverse_cdf(x):
  """Returns the normal distribution inverse CDF.

  Args:
    x: The probability for which to compute the inverse CDF.

  Returns:
    The value v such that the probability that the normal distribution with
    the given parameters is less than or equal to v is equal to x.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.sigmoid',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.half),
        constraints.expect_numerics(P.slope),
        constraints.expect_broadcast_compatible(P.x, P.half, P.slope),
    ],
    qtype_inference_expr=constraints.common_float_qtype_expr(
        P.x, P.half, P.slope
    ),
)
def sigmoid(x, half=0.0, slope=1.0):
  """Returns sigmoid of x element-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._sum',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
        (
            P.initial
            == M_qtype.optional_like_qtype(M_qtype.get_scalar_qtype(P.x)),
            (
                'initial must be optional and have the same scalar qtype as x,'
                f' got {constraints.name_type_msg(P.initial)},'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into),
        M_qtype.get_scalar_qtype(P.x),
    ),
)
def _sum(x, into, initial):
  """(internal) Returns the sums of non-missing elements group-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.sum',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def sum_(x, into=arolla.unspecified()):
  """Returns the sums of non-missing elements group-wise.

  For empty groups the result is 0.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  initial = M_core.cast_values(
      arolla.optional_int32(0), M_qtype.scalar_qtype_of(x)
  )
  return arolla.types.DispatchOperator(
      'x, into, initial',
      # When `into` is an edge-to-scalar and `initial` is a non-optional scalar,
      # the result is non-optional.
      sum_to_scalar=arolla.types.DispatchCase(
          M_core.get_optional_value(_sum(P.x, P.into, P.initial)),
          condition=M_qtype.is_edge_to_scalar_qtype(P.into),
      ),
      default=_sum,
  )(
      x,
      M_core.default_if_unspecified(into, M_edge.to_scalar(x)),
      initial,
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math._sum_sparse',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def _sum_sparse(x, into=arolla.unspecified()):
  """Returns the sums of non-missing elements group-wise.

  Unlike math.sum, for empty groups the result is missing.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  initial = M_core.cast_values(
      arolla.optional_int32(None), M_qtype.scalar_qtype_of(x)
  )
  return _sum(
      x,
      M_core.default_if_unspecified(into, M_edge.to_scalar(x)),
      initial,
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._mean',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into),
        M_qtype.get_float_qtype(M_qtype.get_scalar_qtype(P.x)),
    ),
)
def _mean(x, into):
  """(internal) Returns the means of non-missing elements group-wise."""
  # TODO: Consider supporting integer inputs in the backend.
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.mean',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_numerics(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def mean(x, into=arolla.unspecified()):
  """Returns the means of non-missing elements group-wise.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  return _mean(x, M_core.default_if_unspecified(into, M_edge.to_scalar(x)))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._weighted_average',
    qtype_inference_expr=M_qtype.conditional_qtype(
        M_qtype.is_edge_to_scalar_qtype(P.into),
        M_qtype.get_scalar_qtype(
            constraints.common_float_qtype_expr(P.values, P.weights)
        ),
        M_qtype.with_value_qtype(
            M_qtype.get_parent_shape_qtype(P.into),
            M_qtype.get_scalar_qtype(
                constraints.common_float_qtype_expr(P.values, P.weights)
            ),
        ),
    ),
)
def _weighted_average_backend(values, weights, into):
  """Backend proxy without default arg handling."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.weighted_average',
    qtype_constraints=[
        constraints.expect_array(P.values),
        constraints.expect_numerics(P.values),
        constraints.expect_array(P.weights),
        constraints.expect_numerics(P.weights),
        *constraints.expect_edge_or_unspecified(
            P.into, child_side_param=P.values
        ),
    ],
)
def weighted_average(values, weights, into=arolla.unspecified()):
  """Aggregates a weighted average along the edge `into`.

  Fast dot product of values and weights divided by sum of weights. Indices with
  either a missing value or a missing weight are ignored.
  If sum of weights is 0, outputs inf, -inf or nan based on usual floating-point
  rules.

  Args:
    values: values to be averaged.
    weights: weights for the average.
    into: An edge indicating how to aggregate the values.

  Returns:
    Array with average for each group, or scalar if edge was an edge to scalar.
  """
  return _weighted_average_backend(
      values,
      weights,
      M_core.default_if_unspecified(into, M_edge.to_scalar(values)),
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._max',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), M_qtype.get_scalar_qtype(P.x)
    ),
)
def _max(x, into):
  """(internal) Returns the maximum of non-missing elements group-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.max',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_numerics(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def max_(x, into=arolla.unspecified()):
  """Returns the maximum of non-missing elements group-wise.

  This operator ignores the missing values. The result is present iff there is
  at least one present value.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  return _max(x, M_core.default_if_unspecified(into, M_edge.to_scalar(x)))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._min',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), M_qtype.get_scalar_qtype(P.x)
    ),
)
def _min(x, into):
  """(internal) Returns the minimum of non-missing elements group-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.min',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_numerics(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def min_(x, into=arolla.unspecified()):
  """Returns the minimum of non-missing elements group-wise.

  This operator ignores the missing values. The result is present iff there is
  at least one present value.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  return _min(x, M_core.default_if_unspecified(into, M_edge.to_scalar(x)))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._median',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_numerics(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), M_qtype.get_scalar_qtype(P.x)
    ),
)
def _median(x, into):
  """(internal) Returns the median of non-missing elements group-wise."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.median',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_numerics(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def median(x, into=arolla.unspecified()):
  """Returns the median of non-missing elements group-wise.

  This operator ignores the missing values. The result is present iff there is
  at least one present value.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  return _median(x, M_core.default_if_unspecified(into, M_edge.to_scalar(x)))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._prod',
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into),
        M_qtype.get_scalar_qtype(P.x),
    ),
)
def _prod_backend(x, into):
  """Backend proxy without default arg handling."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.prod',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def prod(x, into=arolla.unspecified()):
  """Returns the product of non-missing elements group-wise.

  This operator ignores the missing values. The result is present iff there is
  at least one present value.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  return _prod_backend(x, into)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.cum_sum',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
    qtype_inference_expr=P.x,
)
def cum_sum(x, over):
  """Returns the cumulative sum of `x` along the edge `over`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.cum_min',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
    qtype_inference_expr=P.x,
)
def cum_min(x, over):
  """Returns the cumulative min of `x` along the edge `over`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math.cum_max',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
    qtype_inference_expr=P.x,
)
def cum_max(x, over):
  """Returns the cumulative max of `x` along the edge `over`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.var',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        constraints.expect_scalar_boolean(P.unbiased),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def var(x, into=arolla.unspecified(), unbiased=True):
  """Returns the variance of non-missing elements group-wise.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
    unbiased: Indicates whether to use an unbiased estimation of variance.
  """
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  differences = x - M_array.expand(mean(x, into), into)
  counts = M_array.count(differences, into)
  # common_qtype(int, weak_float) is float32, so we cast `counts` explicitly.
  counts = M_core.cast_values(
      counts, M_qtype.get_float_qtype(M_qtype.scalar_qtype_of(x))
  )
  ddof = M_core.cast_values(unbiased, arolla.WEAK_FLOAT)  # unbiased ? 1. : 0.

  numerator = _sum_sparse(differences * differences, into)
  denominator = (counts & (counts > ddof)) - ddof
  return numerator / denominator


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.std',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        constraints.expect_scalar_boolean(P.unbiased),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def std(x, into=arolla.unspecified(), unbiased=True):
  """Returns the standard deviations of non-missing elements along the edge.

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
    unbiased: A boolean flag indicating whether to substract 1 in the formula.
  """
  return pow_(var(x, into=into, unbiased=unbiased), arolla.weak_float(0.5))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.skew',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def skew(x, into=arolla.unspecified()):
  r"""Returns the skewness of non-missing elements group-wise.

  It is also known as Pearson's moment coefficient of skewness.

  The simple formula is thus:
  S = \sqrt(n) * (\sum_{i=1}^n [(x_i - avg)^3])
                / (\sum_{i=n}^n [(x_i - avg)^2])^{3/2}
  Python code for testing:
  mean = sum(a) / len(a)
  m2 = sum([(i - mean(a))**2 for i in a])
  m3 = sum([(i - mean(a))**3 for i in a])
  skew = (len(a)**0.5) * m3(a) / (m2(a)**(3./2)) if m2(a) > 0 else 0

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  differences = x - M_array.expand(mean(x, into), into)
  m2 = _sum_sparse(differences * differences, into)
  m3 = _sum_sparse(differences * differences * differences, into)
  count = M_array.count(differences, into)

  # common_qtype(int, weak_float) is float32, so we cast `count` explicitly.
  count = M_core.cast_values(
      count, M_qtype.get_float_qtype(M_qtype.scalar_qtype_of(x))
  )

  return (count ** arolla.weak_float(0.5) * m3) / m2 ** arolla.weak_float(1.5)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.kurtosis',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def kurtosis(x, into=arolla.unspecified()):
  r"""Returns the kurtosis of non-missing elements group-wise.

  We actually compute the excess kurtosis which is simply Kurtosis - 3.0. Excess
  Kurtosis appears more frequently in the literature as the kurtosis relative to
  normal distribution kurtosis. The simple formula above is thus essentially:
  kurtosis = n * (\sum_{i=1}^n [(x_i - avg)^4])
             / (\sum_{i=1}^n [(x_i - avg)^2])^2
             - 3.0
  Python code for testing:
  m4 = lambda a : sum([(i - mean(a))**4 for i in a])
  kurtosis = lambda a : len(a) * m4(a) / (m2(a)**2) - 3.0 if m2(a) > 0 else -3.0

  Args:
    x: An array of numbers.
    into: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  differences = x - M_array.expand(mean(x, into), into)
  differences_squared = differences * differences
  m2 = _sum_sparse(differences_squared, into)
  m4 = _sum_sparse(differences_squared * differences_squared, into)
  count = M_array.count(differences, into)

  # common_qtype(int, weak_float) is float32, so we cast `count` explicitly.
  count = M_core.cast_values(
      count, M_qtype.get_float_qtype(M_qtype.scalar_qtype_of(x))
  )

  return (count * m4) / (m2 * m2) - arolla.weak_float(3.0)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.covariance',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_array(P.y),
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.y),
        constraints.expect_shape_compatible(P.x, P.y),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
        constraints.expect_scalar_boolean(P.unbiased),
    ],
)
def covariance(x, y, into=arolla.unspecified(), unbiased=True):
  """Returns sample covariance between non-missing elements group-wise."""
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  # Follows pandas behaviour and ignore the elements that are not present in
  # both arrays.
  x, y = x & M_core.has(y), y & M_core.has(x)
  x -= M_array.expand(mean(x, into), into)
  y -= M_array.expand(mean(y, into), into)
  x_y = x * y
  count = M_array.count(x_y, into) - M_core.to_int64(unbiased)
  count &= count > arolla.int64(0)
  return sum_(x_y, into) / M_core.cast_values(
      count, M_qtype.scalar_qtype_of(x_y)
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.correlation',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_array(P.y),
        constraints.expect_numerics(P.x),
        constraints.expect_numerics(P.y),
        constraints.expect_shape_compatible(P.x, P.y),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def correlation(x, y, into=arolla.unspecified()):
  """Returns correlation (Pearson) between  non-missing elements group-wise."""
  into = M_core.default_if_unspecified(into, M_edge.to_scalar(x))
  x, y = x & M_core.has(y), y & M_core.has(x)
  x -= M_array.expand(mean(x, into), into)
  y -= M_array.expand(mean(y, into), into)
  x_y = x * y
  x_x = x * x
  y_y = y * y
  result = sum_(x_y, into) / (
      sum_(x_x, into) * sum_(y_y, into)
  ) ** arolla.weak_float(0.5)
  result &= M_array.core_any(M_core.has(x_y), into)
  # Clip to the range [-1, 1].
  result = minimum(maximum(result, arolla.weak_float(-1)), arolla.weak_float(1))
  return result


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'math.softmax',
    qtype_constraints=[
        constraints.expect_numerics(P.x),
        constraints.expect_scalar(P.beta),
        *constraints.expect_edge_or_unspecified(P.over, child_side_param=P.x),
    ],
)
def softmax(x, beta=arolla.weak_float(1.0), over=arolla.unspecified()):
  """Returns the softmax of x element-wise.

  The softmax represents Exp(x * beta) / Sum(Exp(x * beta)) over groups of the
  edge.

  Args:
    x: An array of numbers.
    beta: A floating point number that controls the smooth of the softmax.
    over: An edge indicating how to group the elements. The entire array is
      considered a single group, if the parameter is not specified.
  """
  over = M_core.default_if_unspecified(over, M_edge.to_scalar(x))
  # To avoid overflow in exponentiation we first subtract maximum value from
  # the input. This makes all exponents operate with non-positive power.
  x = x - M.array.expand(M.math.max(x, over), over)
  numerator = exp(beta * x)
  denominator = M.array.expand(M.math.sum(numerator, over), over)
  return numerator / denominator


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'math._add4',
    qtype_constraints=[
        constraints.expect_numerics(P.x0),
        constraints.expect_numerics(P.x1),
        constraints.expect_numerics(P.x2),
        constraints.expect_numerics(P.x3),
        constraints.expect_implicit_cast_compatible(P.x0, P.x1, P.x2, P.x3),
    ],
    qtype_inference_expr=constraints.common_qtype_expr(P.x0, P.x1, P.x2, P.x3),
)
def _add4(x0, x1, x2, x3):
  """(internal) Returns x0 + x1 + x2 + x2 element-wise."""
  raise NotImplementedError('provided by backend')
