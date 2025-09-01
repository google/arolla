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

"""Public python API for Object."""

from __future__ import annotations

from typing import Any as _Any

from arolla import arolla as _arolla
from arolla.objects import clib as _
from arolla.objects.operators import binding_policies as _

M = _arolla.OperatorsContainer(unsafe_extra_namespaces=['objects']).objects

OBJECT = _arolla.abc.invoke_op('objects.make_object_qtype', ())


class Object(_arolla.QValue):
  """Object type storing a mapping from attributes to arbitrary QValues.

  Structured like a JavaScript-like object, optionally containing a prototype
  chain representing a hierarchy.
  * Attribute shadowing is allowed.
  * Attribute retrieval starts at the current Object and traverses up the
    prototype chain until either the attribute is found or the chain ends.
  """

  def __new__(
      cls, prototype: Object | None = None, /, **attrs: _Any
  ) -> 'Object':
    """Constructs an Object with the provided `attrs` and `prototype`."""
    return _arolla.abc.aux_eval_op('objects.make_object', prototype, **attrs)

  def get_attr(
      self, attr: str | _arolla.types.Text, output_qtype: _arolla.QType
  ) -> _arolla.AnyQValue:
    """Returns the value at `attr` with the provided `output_qtype`."""
    return _arolla.abc.infer_attr(
        'objects.get_object_attr',
        (
            _arolla.abc.Attr(qvalue=self),
            _arolla.abc.Attr(qvalue=_arolla.text(attr)),
            _arolla.abc.Attr(qvalue=output_qtype),
        ),
    ).qvalue


_arolla.abc.register_qvalue_specialization(OBJECT, Object)
