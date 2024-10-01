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

"""Tools for conversion from arolla array to numpy array."""

from arolla import arolla
from arolla.experimental import dense_array_numpy_conversion
import numpy as np

_unspecified_fill_value = object()

_NUMERIC_NP_DTYPES_FN = {
    arolla.INT32: np.int32,
    arolla.INT64: np.int64,
    arolla.types.UINT64: np.uint64,
    arolla.FLOAT32: np.float32,
    arolla.FLOAT64: np.float64,
    arolla.WEAK_FLOAT: np.float64,
}

_present_indices_as_dense_array_expr = arolla.M.array.as_dense_array(
    arolla.M.array.present_indices(arolla.L.x)
)
_present_values_as_dense_array_expr = arolla.M.array.as_dense_array(
    arolla.M.array.present_values(arolla.L.x)
)


def present_indices_as_numpy_array(array):
  return dense_array_numpy_conversion.numpy_ndarray_from_dense_array(
      arolla.abc.eval_expr(_present_indices_as_dense_array_expr, x=array)
  )


def present_values_as_numpy_array(array):
  return dense_array_numpy_conversion.numpy_ndarray_from_dense_array(
      arolla.abc.eval_expr(_present_values_as_dense_array_expr, x=array)
  )


def _as_numpy_array_numeric(array, *, fill_value):
  """Converts arolla array to a numpy array of the corresponding dtype.

  Works only for numeric arrays.

  Args:
    array: arolla array to convert.
    fill_value: value to use for missing values. If not specified, the default
      value for the array's value_qtype is used.

  Returns:
    numpy array.
  """
  dtype_fn = _NUMERIC_NP_DTYPES_FN[array.value_qtype]
  try:
    fill_value = dtype_fn(fill_value)
  except (TypeError, ValueError) as exc:
    raise ValueError(
        f'unable to use fill_value={fill_value!r} with {array.qtype}'
    ) from exc
  size = array.size
  count = array.present_count
  if count == 0:
    return np.full(size, dtype_fn(fill_value), dtype=dtype_fn)
  values = present_values_as_numpy_array(array)
  assert values.dtype == dtype_fn, f'{dtype_fn=} != {values.dtype=}'
  if count == size:
    return values
  indices = present_indices_as_numpy_array(array)
  result = np.full(size, dtype_fn(fill_value), dtype=dtype_fn)
  result[indices] = values
  return result


def _as_numpy_array_mask(array):
  size = array.size
  count = array.present_count
  if count == size:
    return np.full(size, True, dtype=bool)

  result = np.full(size, False, dtype=bool)
  if count != 0:
    indices = present_indices_as_numpy_array(array)
    result[indices] = True
  return result


def as_numpy_array_tribool(array):
  """Converts arolla boolean array to a numpy array of dtype=int8.

  The following elements conversion rule is applied:

  True -> 1
  False -> -1
  None -> 0

  Args:
    array: arolla boolean array to convert.

  Returns:
    converted numpy array of dtype=int8.

  Raises:
    TypeError: if array is not a boolean array.
  """
  if array.value_qtype != arolla.BOOLEAN:
    raise TypeError(f'expected boolean array, got {array.qtype}')

  result = np.full(array.size, 0, dtype=np.int8)
  if array.present_count != 0:
    # Arrays do not support bool(array) operation.
    # pylint: disable=singleton-comparison
    result[present_indices_as_numpy_array(array == True)] = 1
    result[present_indices_as_numpy_array(array == False)] = -1
    # pylint: enable=singleton-comparison
  return result


def _as_numpy_array_object(array):
  assert arolla.is_array_qtype(array.qtype)
  return np.array(array.py_value(), dtype=object)


def as_numpy_array(array, *, fill_value=_unspecified_fill_value):
  """Converts arolla array to a numpy array.

  The corresponding dtype of the input array is used.

  Missing values are filled with the specified fill_value only for numeric
  arrays; for other types, fill_value is not used and an error will be thrown
  if it is specified.

  Boolean arrays are converted to a numpy array of dtype=object with elements
  True, False or None.

  Args:
    array: arolla array to convert.
    fill_value: value to use for missing values. If not specified, the default
      value for the array's value_qtype is used.

  Returns:
    converted numpy array.

  Raises:
    TypeError: if array is not an array, or fill_value is specified for a
    non-numeric array.
  """
  if not arolla.is_array_qtype(array.qtype):
    raise TypeError(f'expected array, got {array.qtype}')

  if array.value_qtype in _NUMERIC_NP_DTYPES_FN:
    if fill_value is _unspecified_fill_value:
      if array.value_qtype in arolla.types.FLOATING_POINT_QTYPES:
        fill_value = float('nan')
      else:
        fill_value = 0
    return _as_numpy_array_numeric(array, fill_value=fill_value)

  if fill_value is not _unspecified_fill_value:
    raise TypeError(
        f'fill_value cannot be used with non-numerc types: {array.value_qtype}'
    )
  if array.value_qtype == arolla.UNIT:
    return _as_numpy_array_mask(array)
  return _as_numpy_array_object(array)


def as_numpy_array_with_indices(array):
  """Returns numpy arrays with indices and values of the present elements.

  Args:
    array: arolla array to convert.

  Returns:
    A tuple of numpy arrays as (present indices, values array)
  """
  indices = present_indices_as_numpy_array(array)
  values = present_values_as_numpy_array(array)
  return (indices, values)
