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

"""(Private) QValue specialisations for PY_OBJECT qtype.

Please avoid using this module directly. Use arolla (preferrably) or
arolla.types.types instead.
"""

from typing import Self

from arolla.abc import abc as rl_abc
from arolla.types.qtype import py_object_qtype as rl_py_object_qtype


class PyObject(rl_abc.QValue):
  """QValue specialization for PY_OBJECT qtype.

  PyObject represents a reference to a opaque python object (e.g. stored in
  an expression).
  """

  __slots__ = ()

  def __new__(cls, obj: object, codec: bytes | None = None) -> Self:
    """Wraps an object as an opaque PY_OBJECT qvalue.

    Args:
      obj: An input object.
      codec: A PyObject serialization codec compatible with
        `rl.types.encode_py_object`. See go/rlv2-py-object-codecs for details.

    Returns:
      A PY_OBJECT instance.

    Raises:
      ValueError: Raised when the input object is already a qvalue (!=
          PY_OBJECT).
    """
    return rl_py_object_qtype.py_object(obj, codec)

  def py_value(self) -> object:
    """Returns the stored py object."""
    return rl_py_object_qtype.unbox_py_object(self)

  def codec(self) -> bytes | None:
    """Returns the stored codec."""
    return rl_py_object_qtype.get_py_object_codec(self)


rl_abc.register_qvalue_specialization(rl_py_object_qtype.PY_OBJECT, PyObject)
