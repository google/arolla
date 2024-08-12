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

"""Declaration of M.experimental.* operators."""

from arolla import arolla
from arolla.operators.standard import array as M_array
from arolla.operators.standard import core as M_core
from arolla.operators.standard import math as M_math

P = arolla.P
constraints = arolla.optools.constraints


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'experimental.agg_moving_average',
    qtype_constraints=[
        constraints.expect_numerics(P.series),
        constraints.expect_scalar_integer(P.window_size),
        *constraints.expect_edge(P.over, child_side_param=P.series),
    ],
    qtype_inference_expr=constraints.common_float_qtype_expr(P.series),
)
def agg_moving_average(series, window_size, over):
  """Moving average operator.

  Takes in a series of values and returns the trailing window moving
  average for the specified window size.

  Args:
    series: A series of floating-point values.
    window_size: Window size.
    over: Edge that separates values by groups.

  Returns: The moving average series.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'experimental.ewma',
    qtype_constraints=[
        constraints.expect_numerics(P.series),
        constraints.expect_scalar_float(P.alpha),
        constraints.expect_scalar_boolean(P.adjust),
        constraints.expect_scalar_boolean(P.ignore_missing),
    ],
    qtype_inference_expr=constraints.common_float_qtype_expr(P.series),
)
def ewma(series, alpha, adjust=True, ignore_missing=False):
  """Exponential weighted average operator.

  Takes in the (time-series) values and returns the exponential weighted moving
  average. The implementation follows the behaviour in

    pd.DataFrame.ewm(alpha, ignore_mising).mean()

  (You can find more information at the link:
     https://pandas.pydata.org/docs/dev/reference/api/pandas.DataFrame.ewm.html)

  Args:
    series: A series of floating-point values.
    alpha: Smoothing factor.
    adjust: (Please check pandas documentation)
    ignore_missing: Ignore missing values in weight calculations.

  Returns: The exponential weighted average series.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('experimental.serial_correlation')
def serial_correlation(series, lag):
  """Computes autocorrelation of `serial` with given `lag`."""
  missing_value = M_array.at(series, arolla.optional_int64(None))
  lag_prefix = M_core.const_with_shape(
      M_array.resize_array_shape(M_core.shape_of(series), lag),
      missing_value,
  )
  series_sliced = M_array.slice_(series, 0, M_array.size_(series) - lag)
  series_shifted = M_array.concat(lag_prefix, series_sliced)
  return M_math.correlation(series, series_shifted)
