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

"""A serialization prototype."""

from arolla.abc import abc as _arolla_abc
from arolla.s11n import clib as _arolla_s11n_clib

# Encodes the given values and expressions into a bytes object.
dumps_many = _arolla_s11n_clib.dumps_many

# Decodes values and expressions from the given data.
loads_many = _arolla_s11n_clib.loads_many

# Encodes the given set of named expressions into a bytes object.
dumps_expr_set = _arolla_s11n_clib.dumps_expr_set

# Decodes a set of named expressions from the given data.
loads_expr_set = _arolla_s11n_clib.loads_expr_set

# Encodes multiple values and expressions into riegeli container data.
riegeli_dumps_many = _arolla_s11n_clib.riegeli_dumps_many

# Decodes values and expressions from riegeli container data.
riegeli_loads_many = _arolla_s11n_clib.riegeli_loads_many


def dumps(x: _arolla_abc.QValue | _arolla_abc.Expr, /) -> bytes:
  """Encodes the given value or expression."""
  if isinstance(x, _arolla_abc.QValue):
    return dumps_many(values=(x,), exprs=())
  if isinstance(x, _arolla_abc.Expr):
    return dumps_many(values=(), exprs=(x,))
  raise TypeError(
      'expected a value or an expression, got x:'
      f' {_arolla_abc.get_type_name(type(x))}'
  )


def loads(data: bytes, /) -> _arolla_abc.QValue | _arolla_abc.Expr:
  """Decodes a single value or expression."""
  values, exprs = loads_many(data)
  if len(values) + len(exprs) != 1:
    raise ValueError(
        'expected a value or an expression, got'
        f' {len(values)} value{"" if len(values) == 1 else "s"} and'
        f' {len(exprs)} expression{"" if len(exprs) == 1 else "s"}'
    )
  if values:
    return values[0]
  return exprs[0]


def riegeli_dumps(
    x: _arolla_abc.QValue | _arolla_abc.Expr, /, *, riegeli_options: str = ''
) -> bytes:
  """Encodes a single value or expression into a riegeli container.

  Args:
    x: The value or expression to serialize.
    riegeli_options: A string with riegeli/records writer options. See
      https://github.com/google/riegeli/blob/master/doc/record_writer_options.md
        for details. If not provided, default options will be used.

  Returns:
    A bytes object containing the serialized data in riegeli format.
  """
  if isinstance(x, _arolla_abc.QValue):
    return riegeli_dumps_many(
        values=(x,), exprs=(), riegeli_options=riegeli_options
    )
  if isinstance(x, _arolla_abc.Expr):
    return riegeli_dumps_many(
        values=(), exprs=(x,), riegeli_options=riegeli_options
    )
  raise TypeError(
      'expected a value or an expression, got x:'
      f' {_arolla_abc.get_type_name(type(x))}'
  )


def riegeli_loads(data: bytes, /) -> _arolla_abc.QValue | _arolla_abc.Expr:
  """Decodes a single value or expression from riegeli container data."""
  values, exprs = riegeli_loads_many(data)
  if len(values) + len(exprs) != 1:
    raise ValueError(
        'expected a value or an expression, got'
        f' {len(values)} value{"" if len(values) == 1 else "s"} and'
        f' {len(exprs)} expression{"" if len(exprs) == 1 else "s"}'
    )
  if values:
    return values[0]
  return exprs[0]
