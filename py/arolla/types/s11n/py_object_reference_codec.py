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

"""PyObject reference codec."""

import itertools
import weakref

from arolla.types.s11n import py_object_s11n
from arolla.types.s11n import registered_py_object_codecs


# session_id (bytes) -> PyObjectReferenceCodec.
_CODEC_FROM_SESSION = weakref.WeakValueDictionary()
_SESSION_COUNTER = itertools.count()


def _get_reference_map(session_id: bytes) -> dict[int, object]:
  try:
    return _CODEC_FROM_SESSION[session_id]._reference_map  # pylint: disable=protected-access
  except KeyError as e:
    raise ValueError(
        f'failed to find codec with {session_id=}. The related'
        ' PyObjectReferenceCodec must be available in the current process'
    ) from e


class _PyObjectReferenceCodec(py_object_s11n.PyObjectCodecInterface):
  """Stateless PyObject serialization codec using object references.

  All objects are serialized through mappings from `id(obj) -> obj`, where
  `id(obj)` is used as the serialization string. Each id -> obj map is local to
  a session and is kept alive as long as a related PyObjectReferenceCodec object
  is alive.
  """

  @classmethod
  def encode(cls, obj: object, session_id: bytes) -> bytes:
    obj_from_id = _get_reference_map(session_id)
    obj_from_id[id(obj)] = obj
    return str(id(obj)).encode()

  @classmethod
  def decode(cls, serialized_obj: bytes, session_id: bytes) -> object:
    obj_from_id = _get_reference_map(session_id)
    try:
      return obj_from_id[int(serialized_obj.decode())]
    except KeyError as e:
      raise ValueError(f'failed to deserialize {serialized_obj=}') from e


class PyObjectReferenceCodec:
  """PyObject serialization codec using object references."""

  def __init__(self):
    """Creates a PyObjectReferenceCodec.

    The ReferenceCodec holds the codec name and a reference map used during
    encoding and decoding, and is associated with a PyObject serialization
    session.

    Example:

        my_reference_codec = PyObjectReferenceCodec()
        # Serialization and deserialization of PyObjects is possible hereafter.
        arolla.abc.PyObject(my_obj, codec=my_reference_codec.name)

    IMPORTANT: The `my_reference_codec` instance _must_ be kept alive for the
    duration of serialization and deserialization.
    """
    self._session_id = f'{next(_SESSION_COUNTER)}'.encode()
    self.name = registered_py_object_codecs.register_py_object_codec(
        'REFERENCE_CODEC',
        _PyObjectReferenceCodec,
        options=self._session_id,
    )
    self._reference_map = {}
    _CODEC_FROM_SESSION[self._session_id] = self
