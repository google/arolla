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

"""Declaration of M.dict.* operators."""

from arolla import arolla
from arolla.operators.standard import array as M_array
from arolla.operators.standard import core as M_core
from arolla.operators.standard import qtype as M_qtype

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'dict._contains',
    qtype_constraints=[
        constraints.expect_array_scalar_or_optional(P.xs),
        *constraints.expect_key_to_row_dict(
            P.key_to_row_dict, keys_compatible_with_param=P.xs
        ),
    ],
    qtype_inference_expr=M_qtype.get_presence_qtype(P.xs),
)
def _contains(key_to_row_dict, xs):
  """(internal) Returns the presence in `key_to_row_dict` for each of `xs`.

  Args:
    key_to_row_dict: Key to row dict.
    xs: A key or an array of keys.

  Returns: for each key provided, present element iff `key_to_row_dict` contains
    the given key.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'dict._get_row',
    qtype_constraints=[
        constraints.expect_array_scalar_or_optional(P.xs),
        *constraints.expect_key_to_row_dict(
            P.key_to_row_dict, keys_compatible_with_param=P.xs
        ),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(
        P.xs, arolla.OPTIONAL_INT64
    ),
)
def _get_row(key_to_row_dict, xs):
  """(internal) Applies given dict to point(s).

  Args:
    key_to_row_dict: Key to row dict.
    xs: A key or an array of keys.

  Returns: for each key provided, a row id for the given key.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'dict._keys',
    qtype_constraints=[*constraints.expect_key_to_row_dict(P.key_to_row_dict)],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.DENSE_ARRAY_SHAPE,
        M_qtype._get_key_to_row_dict_key_qtype(P.key_to_row_dict),  # pylint:disable=protected-access
    ),
)
def _keys(key_to_row_dict):
  """(internal) Returns an array of keys.

  Args:
    key_to_row_dict: Key to row dict.

  Returns: An array of keys.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'dict.keys',
    qtype_constraints=[*constraints.expect_dict(P.d)],
)
def keys(d):
  """Returns a dense array of dict keys.

  Args:
    d: a dict.
  """
  return _keys(M_core.get_first(d))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'dict.values',
    qtype_constraints=[*constraints.expect_dict(P.d)],
)
def values(d):
  """Returns a dense array of dict values.

  Args:
    d: a dict.
  """
  return M_core.get_second(d)


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'dict.get',
    qtype_constraints=[
        *constraints.expect_dict(P.d, keys_compatible_with_param=P.xs),
        constraints.expect_array_scalar_or_optional(P.xs),
    ],
)
def get(d, xs):
  """Returns a dense array of dict values.

  Args:
    d: a dict.
    xs: a scalar or an array of keys to look up.
  """
  keys_dict = M_core.get_first(d)
  values_array = M_core.get_second(d)
  return M_array.at(values_array, _get_row(keys_dict, xs))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'dict._make_key_to_row_dict',
    qtype_constraints=[
        (
            M_qtype.is_dense_array_qtype(P.keys),
            (
                'expected keys to be a dense array, got '
                f'{constraints.name_type_msg(P.keys)}'
            ),
        ),
        (
            M_qtype.get_scalar_qtype(P.keys) != arolla.UNIT,
            (
                'UNIT keys are not supported, got'
                f' {constraints.name_type_msg(P.keys)}'
            ),
        ),
    ],
    qtype_inference_expr=M_qtype._get_key_to_row_dict_qtype(  # pylint:disable=protected-access
        M_qtype.get_scalar_qtype(P.keys)
    ),
)
def _make_key_to_row_dict(keys):  # pylint: disable=redefined-outer-name
  """(internal) Constructs a dict `keys` into their positions in the array.

  Returns an error in case of duplicated keys.

  Args:
    keys: An array of keys

  Returns: A dict from an array of keys into their positions in the array.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'dict.make',
    qtype_constraints=[
        constraints.expect_array(P.keys),
        constraints.expect_array(P.values),
    ],
)
def make(keys, values):  # pylint: disable=redefined-outer-name
  """Constructs a dict with the given `keys` and `values`.

  Returns an error in case of duplicated keys.

  Args:
    keys: An array of keys
    values: An array of keys

  Returns:
    A dict from an array of keys into values.
  """
  result_qtype = M_qtype.make_dict_qtype(
      M_qtype.scalar_qtype_of(keys), M_qtype.scalar_qtype_of(values)
  )
  return M.derived_qtype.downcast(
      result_qtype,
      M_core.make_tuple(
          _make_key_to_row_dict(M_array.as_dense_array(keys)),
          M_array.as_dense_array(values),
      ),
  )
