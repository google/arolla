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

"""Utilities for detecting the input and output types of operators."""

import itertools
from typing import Iterable, Iterator
from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types

DETECT_SIGNATURES_DEFAULT_QTYPES = (
    arolla_types.SCALAR_QTYPES
    + arolla_types.OPTIONAL_QTYPES
    + arolla_types.DENSE_ARRAY_QTYPES
    + arolla_types.ARRAY_QTYPES
) + ((
    arolla_abc.NOTHING,
    arolla_abc.QTYPE,
    arolla_abc.UNSPECIFIED,
    arolla_types.ARRAY_EDGE,
    arolla_types.ARRAY_SHAPE,
    arolla_types.ARRAY_TO_SCALAR_EDGE,
    arolla_types.DENSE_ARRAY_EDGE,
    arolla_types.DENSE_ARRAY_SHAPE,
    arolla_types.DENSE_ARRAY_TO_SCALAR_EDGE,
    arolla_types.OPTIONAL_SCALAR_SHAPE,
    arolla_types.SCALAR_SHAPE,
    arolla_types.SCALAR_TO_SCALAR_EDGE,
))


def _detect_qtype_signatures_impl(
    op: arolla_abc.Operator,
    signature: arolla_abc.Signature,
    possible_qtypes: tuple[arolla_abc.QType, ...],
    arity: int,
) -> Iterator[tuple[arolla_abc.QType, ...]]:
  """(internal) Detects qtype signatures of fixed arity."""
  # Generate a list of possible qtypes per operator parameter.
  arg_n_possible_qtypes = []
  for i, param in enumerate(signature.parameters):
    if param.kind == 'positional-or-keyword':
      if i < arity:
        arg_n_possible_qtypes.append(possible_qtypes)
      elif param.default is not None:
        arg_n_possible_qtypes.append((param.default.qtype,))
      else:
        # No signatures available for this arity because arity is too small and
        # operator's default values do not make up for the difference.
        return
    else:
      assert param.kind == 'variadic-positional'
      arg_n_possible_qtypes.extend(possible_qtypes for _ in range(arity - i))
      break
  if len(arg_n_possible_qtypes) < arity:
    # No signatures available for this arity because arity is too big.
    return
  # Main loop: try out all qtype combinations.
  for qtype_signature in itertools.product(*arg_n_possible_qtypes):
    try:
      output_qtype = arolla_abc.infer_attr(op, qtype_signature).qtype
    except ValueError:
      continue
    if output_qtype is None:
      raise RuntimeError(
          'operator returned no output qtype: '
          f'{op=}, arg_qtypes={qtype_signature}'
      )
    yield (*qtype_signature[:arity], output_qtype)


def detect_qtype_signatures(
    op: arolla_abc.Operator,
    *,
    possible_qtypes: Iterable[
        arolla_abc.QType
    ] = DETECT_SIGNATURES_DEFAULT_QTYPES,
    max_arity: int | None = None,
) -> Iterator[tuple[arolla_abc.QType, ...]]:
  """Yields qtype signatures of an expr operator.

  This function finds the operator type signatures by using a brute-force
  approach based on the set of possible_qtypes.

  IMPORTANT: This function has exponential complexity:

    O(len(possible_qtypes)**max_arity).

  Args:
    op: An operator.
    possible_qtypes: A set of possible qtypes.
    max_arity: The maximum arity to attempt for the operator; by default, it's
      determined from the operator's signature. For variadic operators, this
      parameter is compulsory.

  Yields:
    Tuples with types: (arg_0_qtype, ..., return_qtype)
  """
  possible_qtypes = tuple(possible_qtypes)
  signature = arolla_abc.get_operator_signature(op)
  # Check the signature assumptions.
  var_positional_param = None
  for param in signature.parameters:
    # If there is a var_positional parameter, it has to be the last one.
    assert var_positional_param is None
    if param.kind == 'positional-or-keyword':
      pass
    elif param.kind == 'variadic-positional':
      var_positional_param = param
    else:
      raise AssertionError(f'unexpected parameter kind: {param.kind}')
  if max_arity is None:
    if var_positional_param is None:
      max_arity = len(signature.parameters)
    else:
      raise ValueError(
          'operator has a VAR_POSITIONAL parameter, please specify `max_arity`'
      )
  # Detect qtype signatures of arity up to max_arity.
  result = []
  for arity in range(max_arity + 1):
    result.extend(
        _detect_qtype_signatures_impl(
            op, signature, possible_qtypes, arity=arity
        )
    )
  if not result:
    raise ValueError('found no supported qtype signatures')
  yield from result
