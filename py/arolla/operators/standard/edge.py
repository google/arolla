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

"""Declaration of M.edge.* operators."""

from arolla import arolla
from arolla.operators.standard import array as M_array
from arolla.operators.standard import core as M_core
from arolla.operators.standard import dict as M_dict
from arolla.operators.standard import qtype as M_qtype
from arolla.operators.standard import seq as M_seq

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


_DENSE_ARRAY_EDGE = M_qtype.get_edge_qtype(
    M_qtype.with_value_qtype(M_qtype.DENSE_ARRAY_SHAPE, arolla.UNIT)
)
_ARRAY_EDGE = M_qtype.get_edge_qtype(
    M_qtype.with_value_qtype(M_qtype.ARRAY_SHAPE, arolla.UNIT)
)
_DENSE_ARRAY_TO_SCALAR_EDGE = M_qtype.get_edge_to_scalar_qtype(
    M_qtype.with_value_qtype(M_qtype.DENSE_ARRAY_SHAPE, arolla.UNIT)
)
_ARRAY_TO_SCALAR_EDGE = M_qtype.get_edge_to_scalar_qtype(
    M_qtype.with_value_qtype(M_qtype.ARRAY_SHAPE, arolla.UNIT)
)

from_shape = M_array._edge_from_shape  # pylint: disable=protected-access
from_sizes = M_array._edge_from_sizes  # pylint: disable=protected-access

to_scalar = M_array._edge_to_scalar  # pylint: disable=protected-access
to_single = M_array._edge_to_single  # pylint: disable=protected-access

child_shape = M_array._edge_child_shape  # pylint: disable=protected-access

edge_indices = M_array._edge_indices  # pylint: disable=protected-access


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.mapping',
    qtype_constraints=[(
        M_qtype.is_edge_qtype(P.edge)
        & M.qtype.is_array_shape_qtype(M.qtype.get_child_shape_qtype(P.edge)),
        (
            'expected a non-scalar-to-scalar edge, got'
            f' {constraints.name_type_msg(P.edge)}'
        ),
    )],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_child_shape_qtype(P.edge), arolla.INT64
    ),
)
def mapping(edge):
  """Returns a mapping to range from an Edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.from_mapping',
    qtype_constraints=[
        constraints.expect_array(P.mapping),
        constraints.expect_integers(P.mapping),
        constraints.expect_scalar_integer(P.parent_size),
    ],
    qtype_inference_expr=M_qtype.get_edge_qtype(P.mapping),
)
def from_mapping(mapping, parent_size):  # pylint: disable=redefined-outer-name
  """Returns an edge constructed from a mapping.

  A mapping sequence is a generic way to define an array-to-array edge:

      mapping[child_index]  ->  parent_index

  Each element of `mapping` is a non-negative integer below `parent_size` and
  represents an index of an element in the parent space. Multiple elements in
  the child space may correspond to the same element in the parent space.
  A missing `mapping[child_index]` indicates that the element has no mapping to
  the parent space.

  The child space of the returned edge has the same size as `mapping`.

  Args:
    mapping: An integer array with the mapping.
    parent_size: The parent size (the mapping values must be below this value).

  Returns:
    An edge.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.from_split_points',
    qtype_constraints=[
        constraints.expect_array(P.split_points),
        constraints.expect_integers(P.split_points),
    ],
    qtype_inference_expr=M_qtype.get_edge_qtype(P.split_points),
)
def from_split_points(split_points):
  """Returns a split-points edge.

  Split-points is an efficient representation for a *sorted* mapping, where
  the mapping gets cut into runs of equal elements and we store only the offsets
  of each run.

  The offset of the first run is always 0, and we also append an extra split
  point to encode where the last run ends.

  Example:

    edge = M.edge.from_split_points([0, 4, 7, 8])

    M.edge.mapping(edge))  ->  [0, 0, 0, 0, 1, 1, 1, 2]
                                ^           ^        ^ ^
                                0           4        7 8

    M.edge.parent_size(edge))  ->  3

    x = [0, 1, 2, 3, 4, 5, 6, 7]

    M.array.count(x, edge))  ->  [4, 3, 1]
    M.math.sum(x, edge))     ->  [6, 16, 7]

  Args:
    split_points: An increasing sequence of split-points; the first element has
      to be 0, the last element encodes the edge.child_size, the number of
      elements is edge.parent_size+1.

  Returns:
    A split-points edge.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator('edge.from_sizes_or_shape')
def from_sizes_or_shape(x):
  """Returns an edge constructed from an array of sizes or a shape.

  This operator has two modes:
  1) Construct an edge from an array of non-negative integers where each item
  defines the number of children of the corresponding parent.
  2) Thake a child shape and construct an edge-to-scalar.

  Args:
    x: An integer array or a shape.

  Returns:
    An edge or an edge-to-scalar depends on the input type.
  """
  return arolla.optools.dispatch[from_sizes, from_shape](x)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.child_size',
    qtype_constraints=[
        *constraints.expect_edge(P.edge),
        (
            M_qtype.is_array_shape_qtype(M_qtype.get_child_shape_qtype(P.edge)),
            f'{constraints.name_type_msg(P.edge)} is not supported',
        ),
    ],
)
def child_size(edge):
  """Returns the child domain size of an edge."""
  return M_array.array_shape_size(child_shape(edge))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.sizes',
    qtype_constraints=[*constraints.expect_edge(P.edge)],
    qtype_inference_expr=M_qtype.conditional_qtype(
        M_qtype.is_edge_to_scalar_qtype(P.edge),
        arolla.INT64,  # non-optional scalar
        M_qtype.with_value_qtype(
            M_qtype.get_parent_shape_qtype(P.edge), arolla.INT64
        ),
    ),
)
def sizes(edge):
  """Returns the number of child items of each parent group.

  Args:
    edge: A child-parent edge.

  Returns:
    Group size of each parent.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.parent_shape',
    qtype_constraints=[*constraints.expect_edge(P.edge)],
    qtype_inference_expr=M_qtype.get_parent_shape_qtype(P.edge),
)
def parent_shape(edge):
  """Returns the parent-side shape of an edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.parent_size',
    qtype_constraints=[
        *constraints.expect_edge(P.edge),
        (
            M_qtype.is_array_shape_qtype(
                M_qtype.get_parent_shape_qtype(P.edge)
            ),
            f'{constraints.name_type_msg(P.edge)} is not supported',
        ),
    ],
)
def parent_size(edge):
  """Returns the child domain size of an edge."""
  return M_array.array_shape_size(parent_shape(edge))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge._as_dense_array_edge',
    qtype_constraints=[(
        (P.edge == _ARRAY_EDGE) | (P.edge == _ARRAY_TO_SCALAR_EDGE),
        (
            'expected ARRAY_EDGE or ARRAY_TO_SCALAR_EDGE, got '
            f'{constraints.name_type_msg(P.edge)}'
        ),
    )],
    qtype_inference_expr=M_qtype.conditional_qtype(
        P.edge == _ARRAY_EDGE, _DENSE_ARRAY_EDGE, _DENSE_ARRAY_TO_SCALAR_EDGE
    ),
)
def _as_dense_array_edge(edge):
  """(internal) Converts an array_edge to dense_array_edge."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.as_dense_array_edge',
    qtype_constraints=[*constraints.expect_edge(P.edge)],
)
def as_dense_array_edge(edge):
  """Converts an edge to dense_array_edge."""
  no_op_case = arolla.types.RestrictedLambdaOperator(
      P.edge,
      qtype_constraints=[(
          (P.edge == _DENSE_ARRAY_EDGE)
          | (P.edge == _DENSE_ARRAY_TO_SCALAR_EDGE),
          '',
      )],
  )
  return arolla.optools.dispatch[no_op_case, _as_dense_array_edge](edge)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge._group_by',
    qtype_constraints=[
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.over, child_side_param=P.x),
    ],
    qtype_inference_expr=M_qtype.get_edge_qtype(P.x),
)
def _group_by(x, over):
  """(internal) Returns an edge that maps array into unique elements in it."""
  raise NotImplementedError('provided by backend')


# TODO: add a check that all elements in P.x are arrays of the same
# size. And P.over child space also has the same size.
@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge._group_by_tuple',
    qtype_constraints=[
        (
            M_qtype.is_tuple_qtype(P.x),
            f'expected tuple, got {constraints.name_type_msg(P.x)}',
        ),
        *constraints.expect_edge(P.over),
    ],
)
def _group_by_tuple(x, over):
  """(internal) Returns an edge that maps tuple of arrays into unique tuples."""
  x = M.core.concat_tuples(M.core.make_tuple(over), x)
  return M.core.reduce_tuple(
      arolla.LambdaOperator('edge, x', _group_by(P.x, P.edge)), x
  )


# TODO: add a check that all elements in P.x are arrays of the same
# size. And P.over child space also has the same size (or the size of P.x, if it
# is an array).
@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.group_by',
    qtype_constraints=[
        (
            M_qtype.is_array_qtype(P.x) | M_qtype.is_tuple_qtype(P.x),
            (
                'expected array or tuple of arrays, got'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
        *constraints.expect_edge_or_unspecified(P.over),
    ],
)
def group_by(x, over=arolla.unspecified()):
  """Returns an edge that maps x into unique elements in it.

  Example:

    over = M.edge.from_split_points([0, 3, 5, 7])

    x1 = [1, 1, 1, 1, 2, None,    0]
    x2 = [0, 2, 0, 0, 0,    0, None]
         ^         ^     ^          ^
         0         3     5          7

    # Values at positions `i` and `j` are grouped together iff x1[i] == x1[j]
    # and x2[i] == x2[j], and mapping(over)[i] == mapping(over)[j].
    M.edge.group_by(M.core.make_tuple(x1, x2), over)
      ->  [        # (over, x1, x2)
            0,     # (   0,  1,  0)
            1,     # (   0,  1,  2)
            0,     # (   0,  1,  0)
            2,     # (   1,  1,  0)
            3,     # (   1,  2,  0)
            None,  # None -- some element is missing
            None,  # None
          ]
  Args:
    x: (Array|tuple of Arrays) Arrays whose tuple of elements (basically rows of
      input) are grouped by their values.
    over: Grouping within each element of the parent space.

  Returns:
    An edge that maps x into unique elements in it.
  """
  case_1_op = arolla.LambdaOperator(
      'x, over',
      _group_by(P.x, M.core.default_if_unspecified(P.over, to_scalar(P.x))),
  )
  case_2_op = arolla.LambdaOperator(
      'x, over',
      _group_by_tuple(
          P.x,
          M.core.default_if_unspecified(
              P.over, to_scalar(M.core.get_nth(P.x, 0))
          ),
      ),
  )
  return arolla.optools.dispatch[case_1_op, case_2_op](x, over)


@arolla.optools.as_backend_operator(
    'edge._resize_groups_child_side',
    qtype_inference_expr=M_qtype.get_edge_qtype(
        M_qtype.with_value_qtype(
            M_qtype.get_child_shape_qtype(P.edge), arolla.INT64
        )
    ),
)
def _resize_groups_child_side_backend(edge, size, offsets):
  """Backend proxy without default arg handling."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.resize_groups_child_side',
    qtype_constraints=[
        *constraints.expect_edge(P.edge),
        constraints.expect_integers(P.size),
        # Should not accept (edge_to_scalar, optional_scalar)
        (
            (
                M.qtype.is_array_qtype(P.size)
                & (
                    M.qtype.get_shape_qtype(P.size)
                    == M.qtype.get_parent_shape_qtype(P.edge)
                )
            )
            | M.qtype.is_scalar_qtype(P.size),
            (
                'size parameter should be non-optional scalar or array matching'
                ' edge parent-side shape.'
            ),
        ),
        (
            M_qtype.is_array_shape_qtype(M_qtype.get_child_shape_qtype(P.edge)),
            'edge parameter cannot have scalar as its child space.',
        ),
        (
            (P.offsets == arolla.UNSPECIFIED)
            | (
                M.qtype.is_integral_qtype(P.offsets)
                & (
                    M.qtype.get_shape_qtype(P.offsets)
                    == M.qtype.get_child_shape_qtype(P.edge)
                )
            ),
            (
                'offsets parameter should have default value(unit) or integer'
                ' array matching edge child-side shape'
            ),
        ),
    ],
)
def resize_groups_child_side(edge, size, offsets=arolla.unspecified()):
  """Given a many-to-one edge, resizes each group to a specified size.

  Returns an edge from the new child space to the old child space.

  Args:
    edge: The edge to be resized
    size: The new size of each group
    offsets: An optional array parameter in the shape of the child-side of the
      edge which maps each element to a position inside the resized group. By
      default, elements, retain their position in the group. Offsets should be
      valid (non-negative) and non-duplicate. Offsets greater than or equal to
      the new size of a group denote elements that will be missing in the new
      space.

  Returns:
    An edge from the new/resized child space to the old/original child space.
  """
  offsets = M.core.default_if_unspecified(offsets, M.edge.indices(edge))
  return _resize_groups_child_side_backend(edge, size, offsets)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'edge.resize_groups_parent_side',
    qtype_constraints=[
        *constraints.expect_edge(P.edge),
        constraints.expect_integers(P.size),
        (
            (
                M.qtype.is_array_qtype(P.size)
                & (
                    M.qtype.get_shape_qtype(P.size)
                    == M.qtype.get_parent_shape_qtype(P.edge)
                )
            )
            | M.qtype.is_scalar_qtype(P.size),
            (
                'size parameter should be non-optional scalar or array matching'
                ' edge parent-side shape'
            ),
        ),
        (
            M_qtype.is_array_shape_qtype(M_qtype.get_child_shape_qtype(P.edge)),
            'edge parameter cannot have scalar as its child space',
        ),
    ],
    qtype_inference_expr=P.edge,
)
def resize_groups_parent_side(edge, size):
  """Given a many-to-one edge, resizes each group to a specified size.

  Args:
    edge: The edge to be resized
    size: The new size of each group

  Returns: An edge from the new/resized child space to the parent space.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.as_lambda_operator('edge.from_keys._is_allowed_key_qtype')
def _edge_from_keys_is_allowed_key_qtype(qtype):
  """(internal) Validates key type for edge_from_keys operator."""
  scalar_qtype = M_qtype.get_scalar_qtype(qtype)
  is_allowed_scalar_qtype = (
      (scalar_qtype == arolla.UNIT)
      | (scalar_qtype == arolla.BOOLEAN)
      | (scalar_qtype == arolla.INT32)
      | (scalar_qtype == arolla.INT64)
      | (scalar_qtype == arolla.types.UINT64)
      | (scalar_qtype == arolla.BYTES)
      | (scalar_qtype == arolla.TEXT)
  )
  return is_allowed_scalar_qtype & M_qtype.is_array_qtype(qtype)


def _edge_from_keys_qtype_constraints_helper(
    child_key_qtypes, parent_key_qtypes
):
  """(internal) Returns constraints for arguments of edge_from_keys_for_tuples."""
  qtype_constraints = []
  qtype_constraints.append((
      M_seq.all_(
          M_seq.map_(_edge_from_keys_is_allowed_key_qtype, child_key_qtypes)
      ),
      (
          'expected arrays of UNIT / BOOLEAN / ints / BYTES / TEXT, got'
          f' {constraints.name_type_msg(P.child_keys)}'
      ),
  ))
  qtype_constraints.append((
      M_seq.all_(
          M_seq.map_(_edge_from_keys_is_allowed_key_qtype, parent_key_qtypes)
      ),
      (
          'expected arrays of UNIT / BOOLEAN / ints / BYTES / TEXT, got'
          f' {constraints.name_type_msg(P.parent_keys)}'
      ),
  ))
  qtype_constraints.append((
      M_seq.all_equal(M_seq.map_(M_qtype.get_shape_qtype, child_key_qtypes)),
      (
          'expected arrays of same kind, got'
          f' {constraints.name_type_msg(P.child_keys)}'
      ),
  ))
  qtype_constraints.append((
      M_seq.all_equal(M_seq.map_(M_qtype.get_shape_qtype, parent_key_qtypes)),
      (
          'expected arrays of same kind, got'
          f' {constraints.name_type_msg(P.parent_keys)}'
      ),
  ))
  qtype_constraints.append((
      M_qtype.get_shape_qtype(M_seq.at(child_key_qtypes, arolla.int64(0)))
      == M_qtype.get_shape_qtype(M_seq.at(parent_key_qtypes, arolla.int64(0))),
      (
          'expected arrays of same kind, got'
          f' {constraints.name_type_msg(P.child_keys)} and'
          f' {constraints.name_type_msg(P.parent_keys)}'
      ),
  ))

  child_value_qtypes = M_seq.map_(M.qtype.get_scalar_qtype, child_key_qtypes)
  parent_value_qtypes = M_seq.map_(M.qtype.get_scalar_qtype, parent_key_qtypes)
  common_value_qtypes = M_seq.map_(
      M.qtype.common_qtype, child_value_qtypes, parent_value_qtypes
  )
  qtype_constraints.append(
      (
          M_seq.all_(
              M_seq.map_(M_core.equal, parent_value_qtypes, common_value_qtypes)
          ),
          (
              'got incompatible types'
              f' {constraints.name_type_msg(P.child_keys)} and'
              f' {constraints.name_type_msg(P.parent_keys)}'
          ),
      ),
  )
  return qtype_constraints


def _edge_from_keys(child_keys, parent_keys):
  """(internal) Returns an edge from child_keys to parent_keys."""
  return from_mapping(
      M_dict._get_row(  # pylint: disable=protected-access
          M_dict._make_key_to_row_dict(M_array.as_dense_array(parent_keys)),  # pylint: disable=protected-access
          child_keys,
      ),
      M_array.size_(parent_keys),
  )


def _get_common_array_size_in_tuple(keys):
  sizes_ = M_core.map_tuple(M.array.size, keys)
  sizes_optional = M_core.map_tuple(M.core.to_optional, sizes_)
  reduce_op = arolla.LambdaOperator('result, x', P.result & (P.result == P.x))
  return M.core.reduce_tuple(reduce_op, sizes_optional)


def _edge_from_keys_for_tuples(child_keys, parent_keys):
  """(internal) Returns an edge from child_keys to parent_keys."""
  child_size_optional = _get_common_array_size_in_tuple(child_keys)
  parent_size_optional = _get_common_array_size_in_tuple(parent_keys)

  child_keys = M_core.with_assertion(
      child_keys,
      M_core.has(child_size_optional),
      'expected all arrays in child_keys tuple to have the same size',
  )
  parent_keys = M_core.with_assertion(
      parent_keys,
      M_core.has(parent_size_optional),
      'expected all arrays in parent_keys tuple to have the same size',
  )
  child_size_ = M_core.get_optional_value(child_size_optional)
  parent_size_ = M_core.get_optional_value(parent_size_optional)

  all_keys = M.core.map_tuple(
      arolla.LambdaOperator(
          M_array.concat(
              M_core.cast(
                  M_core.get_first(P.x),
                  M_qtype.qtype_of(M_core.get_second(P.x)),
              ),
              M_core.get_second(P.x),
          )
      ),
      M.core.zip(parent_keys, child_keys),
  )
  all_mapping = mapping(group_by(all_keys))

  # In `all_mapping` items from both parent_keys and child_keys are replaced
  # with the group ids.
  #
  # Example:
  # >>> edge.from_keys(Tuple([2, 3, 1], ['a', 'b', 'b']),
  # >>>                Tuple([1, 1, 2, 2], ['a', 'b', 'a', 'b']))
  #
  # all_keys = Tuple([1, 1, 2, 2, 2, 3, 1], ['a', 'b', 'a', 'b', 'a', 'b', 'b'])
  # all_mapping = [0, 1, 2, 3, 2, 4, 1]
  #
  # Below we construct an edge between the group ids.
  # >>> edge.from_keys([2, 4, 1], [0, 1, 2, 3])
  parent_to_mapping = M_array.slice_(all_mapping, 0, parent_size_)
  child_to_mapping = M_array.slice_(all_mapping, parent_size_, child_size_)

  return _edge_from_keys(child_to_mapping, parent_to_mapping)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.from_keys',
    qtype_constraints=[
        *_edge_from_keys_qtype_constraints_helper(
            M_qtype.get_field_qtypes(
                M_qtype.conditional_qtype(
                    M_qtype.is_tuple_qtype(P.child_keys)
                    & M_qtype.is_tuple_qtype(P.parent_keys),
                    P.child_keys,
                    M_qtype.make_tuple_qtype(P.child_keys),
                )
            ),
            M_qtype.get_field_qtypes(
                M_qtype.conditional_qtype(
                    M_qtype.is_tuple_qtype(P.child_keys)
                    & M_qtype.is_tuple_qtype(P.parent_keys),
                    P.parent_keys,
                    M_qtype.make_tuple_qtype(P.parent_keys),
                )
            ),
        )
    ],
)
def from_keys(child_keys, parent_keys):
  """Returns an edge from child_keys to parent_keys.

  Maps each child element i to an index j, where
  child_keys[i] == parent_keys[j].

  If child_keys and parent_keys are tuples of arrays:
    child_keys = (ck_1, ..., ck_n), parent_keys = (pk_1, ..., pk_n);
  then operator maps child element i to an index j, where:
    ck_1[i] == pk_1[j] and ... and ck_n[i] == pk_n[j]

  Note that sizes of child_keys and parent_keys tuples must be equal, and
  inside each tuple sizes of all the arrays must be equal.

  Example:
  >>> edge.from_keys(Tuple([2, 3, 1], ['a', 'b', 'b']),
  >>>                Tuple([1, 1, 2, 2], ['a', 'b', 'a', 'b']))
  Edge(mapping=[2, None, 1])

  In the example above, parent space forms a mapping:
  {Tuple(1, 'a'): 0, Tuple(1, 'b'): 1, Tuple(2, 'a'): 2, Tuple(2, 'b'): 3}
  And we search for the following keys:
  (Tuple(2, 'a'), Tuple(3, 'b'), Tuple(1, 'b'))

  Args:
    child_keys: The ids of the child elements, or tuple of arrays of such ids.
    parent_keys: The ids of the parent elements, or tuple of arrays of such ids.

  Returns:
    An edge
  """
  case_1_op = arolla.types.RestrictedLambdaOperator(
      'child_keys, parent_keys',
      _edge_from_keys(P.child_keys, P.parent_keys),
      qtype_constraints=[(
          ~M_qtype.is_tuple_qtype(P.child_keys)
          & ~M_qtype.is_tuple_qtype(P.parent_keys),
          'disable',
      )],
  )
  case_2_op = arolla.types.RestrictedLambdaOperator(
      'child_keys, parent_keys',
      _edge_from_keys_for_tuples(P.child_keys, P.parent_keys),
      qtype_constraints=[(
          M_qtype.is_tuple_qtype(P.child_keys)
          & M_qtype.is_tuple_qtype(P.parent_keys),
          'disable',
      )],
  )
  return arolla.optools.dispatch[case_1_op, case_2_op](child_keys, parent_keys)


def _expect_common(initial_value, value_tuple):
  return (
      M.seq.reduce(
          M.qtype.common_qtype,
          M.qtype.get_field_qtypes(value_tuple),
          initial_value,
      )
      != arolla.NOTHING,
      (
          'arguments do not have a common type -'
          f' {constraints.name_type_msg(initial_value)},'
          f' {constraints.variadic_name_type_msg(value_tuple)}'
      ),
  )


# TODO: Support to-scalar edges.
@arolla.optools.add_to_registry_as_overloadable('edge.compose')
def compose(edge, *edges):
  """Returns a single edge composed from multiple edges.

  This operator composes several edges A->B, B->C, ... Y->Z into A->Z, when each
  edge is viewed as a one-to-many parent-to-child mapping. For each edge, the
  parent-side must match the prior (left) child-side in order to be composable.

  Example:
    a = M.edge.from_mapping([2, 1, 0], 3)
    b = M.edge.from_mapping([0, 1, 2, 0, 1, 2], 3)
    c = M.edge.compose(a, b)
      => (mapping = [2, 1, 0, 2, 1, 0], parent_size = 3)

  Args:
    edge: An array-to-array edge.
    *edges: Array-to-array edges to compose with `edge`.
  """
  raise NotImplementedError('overloadable operator')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=P.edge == _ARRAY_EDGE
)
@arolla.optools.as_backend_operator(
    'edge.compose._array',
    qtype_constraints=[_expect_common(P.edge, P.edges)],
    qtype_inference_expr=P.edge,
)
def _compose_array(edge, *edges):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=P.edge == _DENSE_ARRAY_EDGE
)
@arolla.optools.as_backend_operator(
    'edge.compose._dense_array',
    qtype_constraints=[_expect_common(P.edge, P.edges)],
    qtype_inference_expr=P.edge,
)
def _compose_dense_array(edge, *edges):
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'edge.group_by2',
    qtype_constraints=[
        (
            M_qtype.is_array_qtype(P.x) | M_qtype.is_tuple_qtype(P.x),
            (
                'expected array or tuple of arrays, got'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
        *constraints.expect_edge_or_unspecified(P.over),
    ],
)
def group_by2(x, over=arolla.unspecified()):
  """Breaks down each group (defined as `over`) by unique elements of `x`.

  This operator works similarly as `edge.group_by` but returns a
  group-to-parent edge in addition to a child-to-group edge. The edge pair forms
  a decomposition of the given `over` edge.

  Example:
    doc_to_query = M.edge.from_sizes(...)
    result = M.edge.group_by2(L.doc_color, doc_to_query)
    doc_to_color = M.core.get_first(result)
    color_to_query = M.core.get_second(result)
    is_popular_color = M.edge.sizes(doc_to_color) > 10  # color-level feature
    popular_color_count = M.array.count(  # query-level feature
        is_popular_color, color_to_query
    )

  Args:
    x: (Array|tuple of Arrays) Values indicating sub grouping within `over`.
    over: Base grouping of elements.

  Returns:
    A tuple of two edges. When `over` is an A->B edge, the return value is a
    pair of edges (A->G, G->B) where G is a sub-group index space.
  """
  child_to_group = group_by(x, over=over)
  child_to_parent = M.core.default_if_unspecified(
      over, to_single(M.core.const_with_shape(child_shape(child_to_group), 0))
  )
  child_to_group_mapping = M_array.collapse(
      mapping(child_to_parent), into=child_to_group
  )
  group_to_parent = from_mapping(
      child_to_group_mapping, parent_size(child_to_parent)
  )
  return M.core.make_tuple(child_to_group, group_to_parent)
