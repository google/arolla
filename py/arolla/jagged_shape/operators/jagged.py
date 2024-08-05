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

"""Declaration of M.jagged.* operators."""

from arolla import arolla
from arolla.jagged_shape.operators import operators_qexpr_clib as _


M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


def _expect_common_edge(expected_edge_type, edges):
  return (
      M.seq.reduce(
          M.qtype.common_qtype,
          M.qtype.get_field_qtypes(edges),
          expected_edge_type,
      )
      != arolla.NOTHING,
      (
          'arguments do not have a common type - expected_type:'
          f' {expected_edge_type}, {constraints.variadic_name_type_msg(edges)}'
      ),
  )


def _expect_shape(shape):
  return (
      is_jagged_shape_qtype(shape),
      f'expected a jagged shape, got: {constraints.name_type_msg(shape)}',
  )


def _expect_same_qtype(shape, other_shape):
  return (
      shape == other_shape,
      (
          'arguments do not have the same type:'
          f' {constraints.name_type_msg(shape)} and'
          f' {constraints.name_type_msg(other_shape)}'
      ),
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.make_jagged_shape_qtype',
    qtype_constraints=[constraints.expect_qtype(P.edge_qtype)],
    qtype_inference_expr=arolla.QTYPE,
)
def make_jagged_shape_qtype(edge_qtype):
  """Returns a jagged shape qtype from `edge_qtype`, or NOTHING."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.is_jagged_shape_qtype',
    qtype_constraints=[constraints.expect_qtype(P.x)],
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
)
def is_jagged_shape_qtype(x):
  """Returns `present` iff the argument is a jagged shape qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.get_edge_qtype',
    qtype_constraints=[constraints.expect_qtype(P.shape_qtype)],
    qtype_inference_expr=arolla.QTYPE,
)
def get_edge_qtype(shape_qtype):
  """Returns the corresponding edge qtype for the provided shape qtype."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.rank',
    qtype_constraints=[_expect_shape(P.shape)],
    qtype_inference_expr=arolla.INT64,
)
def rank(shape):
  """Returns the rank of a jagged shape."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.array_shape_from_edges',
    qtype_constraints=[_expect_common_edge(arolla.ARRAY_EDGE, P.edges)],
    qtype_inference_expr=make_jagged_shape_qtype(arolla.ARRAY_EDGE),
)
def array_shape_from_edges(*edges):
  """Returns a jagged array shape constructed from multiple edges."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.dense_array_shape_from_edges',
    qtype_constraints=[_expect_common_edge(arolla.DENSE_ARRAY_EDGE, P.edges)],
    qtype_inference_expr=make_jagged_shape_qtype(arolla.DENSE_ARRAY_EDGE),
)
def dense_array_shape_from_edges(*edges):
  """Returns a jagged dense_array shape constructed from multiple edges."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overloadable('jagged.shape_from_edges')
def jagged_shape_from_edges(edge, *edges):
  """Returns a jagged shape constructed from multiple edges."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=P.edge == arolla.ARRAY_EDGE
)
@arolla.optools.as_lambda_operator('jagged.shape_from_edges._array')
def jagged_shape_from_array_edges(edge, *edges):
  return M.core.apply_varargs(array_shape_from_edges, edge, *edges)


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=P.edge == arolla.DENSE_ARRAY_EDGE
)
@arolla.optools.as_lambda_operator('jagged.shape_from_edges._dense_array')
def jagged_shape_from_dense_array_edges(edge, *edges):
  return M.core.apply_varargs(dense_array_shape_from_edges, edge, *edges)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.edge_at',
    qtype_constraints=[
        _expect_shape(P.shape),
        constraints.expect_scalar_integer(P.dim),
    ],
    qtype_inference_expr=get_edge_qtype(P.shape),
)
def edge_at(shape, dim):
  """Returns the edge at `shape.edges[dim]`.

  Args:
    shape: a jagged shape.
    dim: shape dimension to return the edge for. Should be in the range
      -rank(shape) <= dim < rank(shape), where indexing works as in python.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.remove_dims',
    qtype_constraints=[
        _expect_shape(P.shape),
        constraints.expect_scalar_integer(P.from_dim),
    ],
    qtype_inference_expr=P.shape,
)
def remove_dims(shape, from_dim):
  """Returns the `shape` containing the edges `shape.edges[:from_dim]`.

  Args:
    shape: a jagged shape.
    from_dim: start of dimensions to remove, where indexing works as in python.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overloadable('jagged.add_dims')
def add_dims(shape, *edges):
  """Returns `shape` with `edges` appended."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=get_edge_qtype(P.shape) == arolla.ARRAY_EDGE
)
@arolla.optools.as_backend_operator(
    'jagged.add_dims._array',
    qtype_constraints=[_expect_common_edge(arolla.ARRAY_EDGE, P.edges)],
    qtype_inference_expr=P.shape,
)
def _add_dims_array_edges(shape, *edges):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=get_edge_qtype(P.shape) == arolla.DENSE_ARRAY_EDGE
)
@arolla.optools.as_backend_operator(
    'jagged.add_dims._dense_array',
    qtype_constraints=[_expect_common_edge(arolla.DENSE_ARRAY_EDGE, P.edges)],
    qtype_inference_expr=P.shape,
)
def _add_dims_dense_array_edges(shape, *edges):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged._flatten',
    qtype_constraints=[
        _expect_shape(P.shape),
        constraints.expect_scalar_integer(P.from_dim),
        constraints.expect_scalar_integer(P.to_dim),
    ],
    qtype_inference_expr=P.shape,
)
def _flatten(shape, from_dim, to_dim):
  """(Internal) Flattens `shape.edges[from_dim:to_dim]`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'jagged.flatten',
    qtype_constraints=[
        _expect_shape(P.shape),
        constraints.expect_scalar_integer(P.from_dim),
        (
            M.qtype.is_integral_scalar_qtype(P.to_dim)
            | (P.to_dim == arolla.UNSPECIFIED),
            (
                'expected an integer scalar or unspecified, got'
                f' {constraints.name_type_msg(P.to_dim)}'
            ),
        ),
    ],
)
def flatten(shape, from_dim=0, to_dim=arolla.unspecified()):
  """Returns `shape` with `shape.edges[from_dim:to_dim]` flattened.

  Indexing works as in python:
  * If `to_dim` is unspecified, `to_dim = rank(shape)` is used.
  * If `to_dim < from_dim`, `to_dim = from_dim` is used.
  * If `to_dim < 0`, `max(0, to_dim + rank())` is used. The same goes for
    `from_dim`.
  * If `to_dim > rank()`, `rank()` is used. The same goes for `from_dim`.

  The above-mentioned adjustments places both `from_dim` and `to_dim` in the
  range `[0, rank()]`. After adjustments, the new shape has `rank() == old_rank
  - (to_dim - from_dim) + 1`. Note that if `from_dim == to_dim`, a "unit"
  dimension is inserted at `from_dim`.

  Example:
    # Flatten the last two dimensions into a single dimension, producing a shape
    # with `rank = old_rank - 1`.
    shape = M.jagged.from_sizes(..., [2, 1], [7, 5, 3])
    M.jagged.flatten(shape, -2)  # -> M.jagged.from_sizes(..., [12, 3])

    # Flatten all dimensions except the last, producing a shape with `rank = 2`.
    shape = M.jagged.from_sizes(..., [7, 5, 3])
    M.jagged.flatten(shape, 0, -1)  # -> M.jagged.from_sizes([3], [7, 5, 3])

    # Flatten all dimensions.
    shape = M.jagged.from_sizes([3], [7, 5, 3])
    M.jagged.flatten(shape)  # -> M.jagged.from_sizes([15])

  Args:
    shape: a jagged shape.
    from_dim: start of dimensions to flatten.
    to_dim: end of dimensions to flatten. Defaults to `rank()` if unspecified.
  """
  to_dim = M.core.default_if_unspecified(to_dim, rank(shape))
  return _flatten(shape, from_dim, to_dim)


@arolla.optools.add_to_registry_as_overloadable('jagged.edges')
def edges_(shape):
  """Returns the edges of `shape`."""
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=get_edge_qtype(P.shape) == arolla.ARRAY_EDGE
)
@arolla.optools.as_backend_operator(
    'jagged.edges._array',
    qtype_inference_expr=M.qtype.make_sequence_qtype(arolla.ARRAY_EDGE),
)
def _edges_array(shape):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=get_edge_qtype(P.shape) == arolla.DENSE_ARRAY_EDGE
)
@arolla.optools.as_backend_operator(
    'jagged.edges._dense_array',
    qtype_inference_expr=M.qtype.make_sequence_qtype(arolla.DENSE_ARRAY_EDGE),
)
def _edges_dense_array(shape):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.size',
    qtype_constraints=[_expect_shape(P.shape)],
    qtype_inference_expr=arolla.INT64,
)
def size(shape):
  """Returns the total number of elements the jagged shape represents."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.is_broadcastable_to',
    qtype_constraints=[
        _expect_shape(P.shape),
        _expect_shape(P.other_shape),
        _expect_same_qtype(P.shape, P.other_shape),
    ],
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
)
def is_broadcastable_to(shape, other_shape):
  """Returns present iff `shape` is broadcastable to `other_shape`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'jagged.equal',
    qtype_constraints=[
        _expect_shape(P.shape),
        _expect_shape(P.other_shape),
        _expect_same_qtype(P.shape, P.other_shape),
    ],
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
)
def equal(shape, other_shape):
  """Returns present iff `shape` equals to `other_shape`."""
  raise NotImplementedError('provided by backend')
