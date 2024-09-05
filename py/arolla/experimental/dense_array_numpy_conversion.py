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

"""Tools for low-level conversion from arolla dense array to numpy array."""

from arolla import arolla
from arolla.experimental import clib
import numpy as np


_numpy_dtype_from_qtype = {
    arolla.DENSE_ARRAY_BOOLEAN: 'bool_',
    arolla.DENSE_ARRAY_FLOAT32: 'float32',
    arolla.DENSE_ARRAY_FLOAT64: 'float64',
    arolla.DENSE_ARRAY_WEAK_FLOAT: 'float64',
    arolla.DENSE_ARRAY_INT32: 'int32',
    arolla.DENSE_ARRAY_INT64: 'int64',
    arolla.types.DENSE_ARRAY_UINT64: 'uint64',
}


def numpy_ndarray_from_dense_array(dense_array: arolla.abc.QValue, /):
  """Returns the dense array data as a numpy.ndarray."""
  return np.frombuffer(
      clib.get_dense_array_memoryview(dense_array),
      dtype=_numpy_dtype_from_qtype[dense_array.qtype],
  )
