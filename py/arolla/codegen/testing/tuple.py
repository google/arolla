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

"""Collection of test expressions on tuples."""

from arolla import arolla

L = arolla.L
M = arolla.M
P = arolla.P


def floats(*exprs):
  return (M.annotation.qtype(e, arolla.FLOAT32) for e in exprs)


def reduce_tuple():
  """Test case for core.reduce_tuple operator."""
  return M.core.reduce_tuple(
      M.math.divide, M.core.make_tuple(*floats(L.w, L.x, L.y, L.z))
  )


def make_empty_tuple():
  """Test case for core.make_tuple top level."""
  return M.core.make_tuple()


def make_literal_tuple():
  """Test case for core.make_tuple top level."""
  return M.core.make_tuple(1, 2.0, b'3')


def make_flat_tuple():
  """Test case for core.make_tuple top level."""
  return M.core.make_tuple(
      M.annotation.qtype(L.x, arolla.FLOAT32),
      M.annotation.qtype(L.y, arolla.OPTIONAL_INT32),
      M.annotation.qtype(L.z, arolla.BYTES),
  )


def make_nested_tuple():
  """Test case for core.make_tuple nested."""
  res = M.core.make_tuple(
      M.annotation.qtype(L.x, arolla.FLOAT32),
      M.annotation.qtype(L.y, arolla.INT32),
  )
  for i in range(10):
    res = M.core.make_tuple(
        M.annotation.qtype(L.x, arolla.FLOAT32) + float(i + 1),
        M.annotation.qtype(L.y, arolla.INT32) + i + 1,
        res,
    )
  return res
