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

"""Declaration of M.random.* operators."""

from arolla import arolla
from arolla.operators.standard import array as M_array
from arolla.operators.standard import core as M_core
from arolla.operators.standard import edge as M_edge
from arolla.operators.standard import math as M_math
from arolla.operators.standard import qtype as M_qtype
from arolla.operators.standard import strings as M_strings

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'random._cityhash',
    qtype_constraints=[
        constraints.expect_texts(P.x),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.INT64),
)
def _cityhash(x, seed):
  """Returns a hash value between [0, 2^63 - 1] of string 'x' for given seed."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'random.cityhash',
    qtype_constraints=[
        constraints.expect_scalar_integer(P.seed),
    ],
)
def cityhash(x, seed):
  """Returns a hash value between [0, 2^63 - 1] of 'x' for given seed.

  The hash value is generated using CityHash library.

  Args:
    x: value for hash
    seed: seed for hash

  Returns:
    The hash value between [0, 2^63 - 1].
  """
  return _cityhash(
      M_strings.as_text(x), M_core.cast(seed, arolla.INT64, implicit_only=False)
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'random.sample_n',
    qtype_constraints=[
        constraints.expect_array_shape(P.shape),
        constraints.expect_integers(P.n),
        constraints.expect_dict_key_types_or_unspecified(P.key),
        *constraints.expect_edge_or_unspecified(P.over, parent_side_param=P.n),
        (
            (P.shape == M_qtype.get_shape_qtype(P.key))
            | (P.key == arolla.UNSPECIFIED),
            (
                'expected compatible types, got'
                f' {constraints.name_type_msg(P.shape)} and'
                f' {constraints.name_type_msg(P.key)}'
            ),
        ),
    ],
)
def sample_n(
    shape, n, seed, key=arolla.unspecified(), over=arolla.unspecified()
):
  """Returns a mask array of at most n items for groups grouped by 'over' edge.

  When there are less than n items, result mask has the same size as input
  array.

  The sampling results are stable given the same inputs.

  When 'key' is provided, it is used to generate random numbers. If the key is
  missing, the corresponding item in the array is not sampled.

  Args:
    shape: shape of the array to be sampled.
    n: number of sampled items. Can be a scalar or an array when 'over' is set.
    seed: seed from random sampling.
    key: keys used to generate random numbers. The same key generates the same
      random number. Supported types are integer, bytes, text and boolean.
    over: the edge used for grouping.

  Returns:
    A mask array.
  """
  over = M_core.default_if_unspecified(over, M_edge.from_sizes_or_shape(shape))
  key = M_strings.as_text(
      M_core.default_if_unspecified(key, M_array.iota(shape))
  )
  hash_value = cityhash(key, seed)
  return M_array.ordinal_rank(hash_value, over=over) < M_array.expand(n, over)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'random.sample',
    qtype_constraints=[
        constraints.expect_array_shape(P.shape),
        constraints.expect_scalar_float(P.ratio),
        constraints.expect_dict_key_types_or_unspecified(P.key),
        constraints.expect_array_or_unspecified(P.key),
        (
            (P.shape == M_qtype.get_shape_qtype(P.key))
            | (P.key == arolla.UNSPECIFIED),
            (
                'expected compatible types, got'
                f' {constraints.name_type_msg(P.shape)} and'
                f' {constraints.name_type_msg(P.key)}'
            ),
        ),
    ],
)
def sample(shape, ratio, seed, key=arolla.unspecified()):
  """Returns a mask array based on provided sample ratio.

  Note that the sampling is performed as follows:
    hash(key, seed) < ratio * 2^63
  Therefore, exact sampled count is not guaranteed. E,g, result of sampling an
  array of 1000 items with 0.1 ratio has present items close to 100 (e.g. 98)
  rather than exact 100 elements.

  Args:
    shape: shape of the array to be sampled.
    ratio: Fraction of items to sample, [0, 1].
    seed: seed from random sampling.
    key: keys used to generate random numbers. The same key generates the same
      random number.

  Returns:
    A mask array.
  """
  ratio = M_math.minimum(ratio, 1.0)
  key = M_core.default_if_unspecified(key, M_array.iota(shape))
  hash_value = cityhash(M_strings.as_text(key), seed)
  return M_core.to_float64(hash_value) < ratio * arolla.float64(2**63)
