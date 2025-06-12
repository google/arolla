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

"""(Private) QValue specializations for dict types."""

from __future__ import annotations

from typing import Any, Mapping

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import boxing


class Dict(arolla_abc.QValue):
  """QValue specialization for Dict qtypes."""

  __slots__ = ()

  def __new__(
      cls,
      items: Mapping[Any, Any],
      *,
      key_qtype: arolla_abc.QType | None = None,
      value_qtype: arolla_abc.QType | None = None,
  ) -> Dict:
    """Constructs a Dict with the given keys / values.

    Args:
      items: Python dict to convert to QValue
      key_qtype: optional QType of the dict keys, will be decuced if not
        provided.
      value_qtype: optional QType of the dict values, will be decuced if not
        provided.

    Returns:
      constructed Dict QValue.
    """
    keys = boxing.dense_array(items.keys(), key_qtype)
    values = boxing.dense_array(items.values(), value_qtype)
    return arolla_abc.invoke_op('dict.make', (keys, values))

  def keys(self):
    return arolla_abc.invoke_op('dict.keys', (self,))

  def values(self):
    return arolla_abc.invoke_op('dict.values', (self,))

  def py_value(self) -> dict[Any, Any]:
    """Returns a python dict with the same keys and values."""
    keys = array_qtypes.get_array_py_value(
        arolla_abc.invoke_op('dict.keys', (self,))
    )
    values = array_qtypes.get_array_py_value(
        arolla_abc.invoke_op('dict.values', (self,))
    )
    result = dict(zip(keys, values))
    assert len(result) == len(keys) == len(values)
    return result


arolla_abc.register_qvalue_specialization('::arolla::DictQType', Dict)
