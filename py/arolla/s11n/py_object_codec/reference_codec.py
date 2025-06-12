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

"""PyObject reference codec."""

import itertools
import weakref

from arolla.abc import abc as arolla_abc
from arolla.s11n.py_object_codec import tools


# codec_name (bytes) -> ReferencePyObjectCodec.
_CODEC_FROM_SESSION = weakref.WeakValueDictionary()
_SESSION_COUNTER = itertools.count()


def _get_reference_map(codec: bytes) -> dict[int, arolla_abc.PyObject]:
  try:
    return _CODEC_FROM_SESSION[codec]._reference_map  # pylint: disable=protected-access
  except KeyError as e:
    raise ValueError(
        f'failed to find {codec=}. The related ReferencePyObjectCodec must be'
        ' available in the current process'
    ) from e


class _ReferencePyObjectCodec(tools.PyObjectCodecInterface):
  """Stateless PyObject serialization codec using object references.

  All objects are serialized through mappings from `id(obj) -> obj`, where
  `id(obj)` is used as the serialization string. Each id -> obj map is local to
  a session and is kept alive as long as a related ReferencePyObjectCodec object
  is alive.
  """

  @classmethod
  def encode(cls, obj: arolla_abc.PyObject) -> bytes:
    obj_from_id = _get_reference_map(obj.codec())
    obj_id = id(obj.py_value())
    obj_from_id[obj_id] = obj
    return str(obj_id).encode()

  @classmethod
  def decode(cls, serialized_obj: bytes, codec: bytes) -> arolla_abc.PyObject:
    obj_from_id = _get_reference_map(codec)
    try:
      return obj_from_id[int(serialized_obj.decode())]
    except KeyError as e:
      raise ValueError(
          f'failed to deserialize {serialized_obj=}, {codec=}'
      ) from e


class ReferencePyObjectCodec:
  """PyObject serialization codec using object references."""

  def __init__(self):
    """Creates a ReferencePyObjectCodec.

    The ReferenceCodec holds the codec name and a reference map used during
    encoding and decoding, and is associated with a PyObject serialization
    session.

    Example:

        my_reference_codec = ReferencePyObjectCodec()
        # Serialization and deserialization of PyObjects is possible hereafter.
        arolla.abc.PyObject(my_obj, codec=my_reference_codec.name)

    IMPORTANT: The `my_reference_codec` instance _must_ be kept alive for the
    duration of serialization and deserialization.
    """
    self.name = tools.register_py_object_codec(
        f'REFERENCE_CODEC:{next(_SESSION_COUNTER)}', _ReferencePyObjectCodec
    )
    self._reference_map = {}
    _CODEC_FROM_SESSION[self.name] = self
