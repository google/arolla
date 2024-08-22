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

"""Scalar utils test helpers."""

import dataclasses
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
import numpy


@dataclasses.dataclass
class BytesLike:
  """A minimal bytes-like type."""

  value: bytes

  def __bytes__(self) -> bytes:
    return self.value


@dataclasses.dataclass
class FloatLike:
  """A minimal float-like type."""

  value: float

  def __float__(self) -> float:
    return self.value


@dataclasses.dataclass
class IndexLike:
  """A minimal index-like type."""

  value: int

  def __index__(self) -> int:
    return self.value


# go/keep-sorted start block=yes newline_separated=yes
BOOLEAN_TEST_DATA = (
    (None, None),
    (False, False),
    (True, True),
    (scalar_qtypes.boolean(False), False),
    (optional_qtypes.optional_boolean(None), None),
    (optional_qtypes.optional_boolean(True), True),
    (numpy.bool_(True), True),
)

BYTES_TEST_DATA = (
    (None, None),
    (b'', b''),
    (b'abra', b'abra'),
    (scalar_qtypes.bytes_(b'ca'), b'ca'),
    (optional_qtypes.optional_bytes(None), None),
    (optional_qtypes.optional_bytes(b'da'), b'da'),
    (numpy.bytes_(b'br'), b'br'),
    (BytesLike(b'a'), b'a'),
)

FLOAT_TEST_DATA = (
    (None, None),
    (0, 0.0),
    (0.0, 0.0),
    (1, 1.0),
    (1.5, 1.5),
    (scalar_qtypes.int32(2), 2.0),
    (scalar_qtypes.int64(3), 3.0),
    (scalar_qtypes.uint64(4), 4.0),
    (scalar_qtypes.float32(4.5), 4.5),
    (scalar_qtypes.float64(5.5), 5.5),
    (scalar_qtypes.weak_float(6.5), 6.5),
    (optional_qtypes.optional_int32(None), None),
    (optional_qtypes.optional_int32(7), 7.0),
    (optional_qtypes.optional_int64(None), None),
    (optional_qtypes.optional_int64(8), 8.0),
    (optional_qtypes.optional_uint64(None), None),
    (optional_qtypes.optional_uint64(9), 9.0),
    (optional_qtypes.optional_float32(None), None),
    (optional_qtypes.optional_float32(9.5), 9.5),
    (optional_qtypes.optional_float64(None), None),
    (optional_qtypes.optional_float64(10.5), 10.5),
    (optional_qtypes.optional_weak_float(None), None),
    (optional_qtypes.optional_weak_float(11.5), 11.5),
    (IndexLike(12), 12.0),
    (FloatLike(-1.0), -1.0),
)

INDEX_TEST_DATA = (
    (None, None),
    (0, 0),
    (1, 1),
    (scalar_qtypes.int32(2), 2),
    (scalar_qtypes.int64(3), 3),
    (scalar_qtypes.uint64(4), 4),
    (optional_qtypes.optional_int32(None), None),
    (optional_qtypes.optional_int32(5), 5),
    (optional_qtypes.optional_int64(None), None),
    (optional_qtypes.optional_int64(6), 6),
    (optional_qtypes.optional_uint64(None), None),
    (optional_qtypes.optional_uint64(7), 7),
    (IndexLike(8), 8),
)

TEXT_TEST_DATA = (
    (None, None),
    ('', ''),
    ('abra', 'abra'),
    (scalar_qtypes.text('ca'), 'ca'),
    (optional_qtypes.optional_text(None), None),
    (optional_qtypes.optional_text('da'), 'da'),
    (numpy.str_('bra'), 'bra'),
)

UNIT_TEST_DATA = (
    (None, None),
    (True, True),
    (scalar_qtypes.unit(), True),
    (optional_qtypes.optional_unit(None), None),
    (optional_qtypes.optional_unit(True), True),
)

# go/keep-sorted end
