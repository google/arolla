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

"""PY_OBJECT qtype."""

from typing import Self

from arolla.abc import clib
from arolla.abc import qtype as abc_qtype

PY_OBJECT = clib.internal_make_py_object_qvalue(object()).qtype


class PyObject(abc_qtype.QValue):
  """QValue specialization for PY_OBJECT qtype.

  PyObject represents a reference to a opaque python object (e.g. stored in
  an expression).
  """

  __slots__ = ()

  def __new__(cls, value: object, codec: bytes | None = None) -> Self:
    """Wraps an object as an opaque PY_OBJECT qvalue.

    Args:
      value: An input object.
      codec: A PyObject serialization codec compatible with
        `rl.types.encode_py_object`. See go/rlv2-py-object-codecs for details.

    Returns:
      A PY_OBJECT instance.

    Raises:
      ValueError: Raised when the input object is already a qvalue.
    """
    return clib.internal_make_py_object_qvalue(value, codec)

  def py_value(self) -> object:
    """Returns the stored py object."""
    return clib.internal_get_py_object_value(self)

  def codec(self) -> bytes | None:
    """Returns the stored codec."""
    return clib.internal_get_py_object_codec(self)


abc_qtype.register_qvalue_specialization(PY_OBJECT, PyObject)
