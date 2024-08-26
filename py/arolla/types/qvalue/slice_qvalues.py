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

"""(Private) QValue specialisations for Slice."""

from __future__ import annotations

from typing import Any

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import boxing
from arolla.types.qtype import tuple_qtypes


class Slice(arolla_abc.QValue):
  """QValue specialization for Slice qtypes."""

  __slots__ = ()

  def __new__(
      cls, start: Any = None, stop: Any = None, step: Any = None
  ) -> Slice:
    """Constructs a slice(start, stop, step)."""
    return arolla_abc.invoke_op(
        'core.make_slice',
        (
            boxing.as_qvalue(start),
            boxing.as_qvalue(stop),
            boxing.as_qvalue(step),
        ),
    )

  @property
  def start(self) -> arolla_abc.AnyQValue:
    return tuple_qtypes.get_nth(self, 0)

  @property
  def stop(self) -> arolla_abc.AnyQValue:
    return tuple_qtypes.get_nth(self, 1)

  @property
  def step(self) -> arolla_abc.AnyQValue:
    return tuple_qtypes.get_nth(self, 2)

  def py_value(self):
    """Returns a python slice with py_values."""
    return slice(
        self.start.py_value(), self.stop.py_value(), self.step.py_value()
    )


arolla_abc.register_qvalue_specialization('::arolla::SliceQType', Slice)
