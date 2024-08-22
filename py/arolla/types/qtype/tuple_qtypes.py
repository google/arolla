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

"""(Private) Tuple related functions."""

from arolla.abc import abc as arolla_abc
from arolla.types.qtype import clib


# Returns True iff the given qtype is a tuple.
is_tuple_qtype = clib.is_tuple_qtype

# Returns True iff the given qtype is a namedtuple.
is_namedtuple_qtype = clib.is_namedtuple_qtype

# Returns the index of the field by its name, or None if no such field exists.
get_namedtuple_field_index = clib.get_namedtuple_field_index

# Returns the field names of the namedtuple type.
get_namedtuple_field_names = clib.get_namedtuple_field_names

# Returns n-th field of the value.
get_nth = clib.get_nth


def make_tuple_qtype(*field_qtypes: arolla_abc.QType) -> arolla_abc.QType:
  """Returns a tuple qtype with the given field types."""
  return clib.internal_make_tuple_qtype(field_qtypes)


def make_namedtuple_qtype(**field_qtypes: arolla_abc.QType) -> arolla_abc.QType:
  """Returns a namedtuple qtype with the given field qtypes."""
  return clib.internal_make_namedtuple_qtype(
      list(field_qtypes.keys()),
      clib.internal_make_tuple_qtype(list(field_qtypes.values())),
  )


def make_tuple_qvalue(
    *field_qvalues: arolla_abc.QValue,
) -> arolla_abc.AnyQValue:
  """Returns a tuple with the given field values."""
  tuple_qtype = clib.internal_make_tuple_qtype(
      [field_qvalue.qtype for field_qvalue in field_qvalues]
  )
  return clib.make_qvalue_from_fields(tuple_qtype, field_qvalues)


def make_namedtuple_qvalue(
    **field_qvalues: arolla_abc.QValue,
) -> arolla_abc.AnyQValue:
  """Returns a namedtuple with the given field values."""
  tuple_qtype = clib.internal_make_tuple_qtype(
      [field_qvalue.qtype for field_qvalue in field_qvalues.values()]
  )
  namedtuple_qtype = clib.internal_make_namedtuple_qtype(
      list(field_qvalues.keys()), tuple_qtype
  )
  return clib.make_qvalue_from_fields(
      namedtuple_qtype, list(field_qvalues.values())
  )


_EMPTY_NAMEDTUPLE_QTYPE = make_namedtuple_qtype()


def get_namedtuple_field_qtypes(
    qtype: arolla_abc.QType,
) -> dict[str, arolla_abc.QType]:
  """Returns a dict field qtypes of a namedtuple."""
  if not is_namedtuple_qtype(qtype):
    raise TypeError(f'expected a namedtuple qtype, got {qtype}')
  # NOTE: `arolla.abc.get_field_qtypes` fails on an empty namedtuple because
  # it's technically a type without fields. So we handle this case specially.
  # We can reconsider this in the future.
  if qtype == _EMPTY_NAMEDTUPLE_QTYPE:
    return {}
  return dict(
      zip(
          clib.get_namedtuple_field_names(qtype),
          arolla_abc.get_field_qtypes(qtype),
      )
  )
