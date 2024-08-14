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

"""A toy type derived from int32 for testing purposes."""

import functools

from arolla import arolla
from arolla.examples.gf import gf127 as _gf127

_M = arolla.M

# Type for GF[127].
# (https://en.wikipedia.org/wiki/Finite_field)
GF127 = _gf127.GF127


@arolla.optools.as_lambda_operator('_as_gf127')
def _as_gf127(x):
  return _M.derived_qtype.downcast(GF127, x)


@arolla.optools.as_lambda_operator('_as_int32')
def _as_int32(x):
  return _M.derived_qtype.upcast(GF127, x)


@arolla.optools.as_lambda_operator('gf127')
def gf127(x):
  """Casting to GF127."""
  return _as_gf127(_M.core.to_int32(x % arolla.int32(127)))


@arolla.optools.as_lambda_operator('gf127_add')
def add(l, r):
  """l + r (mod 127)."""
  return gf127(_as_int32(l) + _as_int32(r))


@arolla.optools.as_lambda_operator('gf127_sub')
def sub(l, r):
  """l - r (mod 127)."""
  return gf127(_as_int32(l) - _as_int32(r))


@arolla.optools.as_lambda_operator('gf127_mul')
def mul(l, r):
  """l * r (mod 127)."""
  return gf127(_as_int32(l) * _as_int32(r))


def _pow125(x: arolla.Expr) -> arolla.Expr:
  pow1 = x
  pow2 = mul(pow1, pow1)
  pow4 = mul(pow2, pow2)
  pow8 = mul(pow4, pow4)
  pow16 = mul(pow8, pow8)
  pow32 = mul(pow16, pow16)
  pow64 = mul(pow32, pow32)
  return functools.reduce(mul, [pow1, pow4, pow8, pow16, pow32, pow64])


@arolla.optools.as_lambda_operator('gf127_inv')
def inv(x):
  """1 / x (mod 127)."""
  return _pow125(x)


@arolla.optools.as_lambda_operator('gf127_div')
def div(l, r):
  """l / r (mod 127)."""
  return mul(l, inv(r))


@arolla.optools.as_lambda_operator('gf127_array_at')
def array_at(array, i):
  """array[i]."""
  return _as_gf127(_M.array.at(_as_int32(array), i))
