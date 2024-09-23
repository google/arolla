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

from typing import Iterable, Iterator

from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types

QTypeSignature = tuple[arolla_abc.QType, ...]

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


def _detect_qtype_signatures_with_fixed_arity(
    op: arolla_abc.Operator,
    possible_qtypes: Iterable[arolla_abc.QType],
    signature: arolla_abc.Signature,
    arity: int,
) -> Iterator[QTypeSignature]:
  """(internal) Detects qtype signatures with a specific arity."""
  input_qtypes = [None] * arity
  # Initializes `input_qtypes`; the first `arity` positions will be tried with
  # all `possible_qtypes` combinations. The rest need to be covered with
  # the parameters' default values.
  for i in range(arity, len(signature.parameters)):
    param = signature.parameters[i]
    if param.kind == 'positional-or-keyword':
      if param.default is None:
        # One of the parameters has no default value, so we cannot initiate
        # the search process.
        return
      input_qtypes.append(param.default.qtype)

  # Pre-filter the possible qtypes for each input to help reduce the search
  # space for the main search.
  possible_qtypes_per_input = [[] for _ in range(arity)]
  for i in range(arity):
    for qtype in possible_qtypes:
      try:
        input_qtypes[i] = qtype
        arolla_abc.infer_attr(op, tuple(input_qtypes))
        possible_qtypes_per_input[i].append(qtype)
      except ValueError:
        pass
    input_qtypes[i] = None

  # The main search tries all possible types for the i-th input, and
  # if the operator doesn't discard it, recursively proceed to
  # the (i+1)-th input.
  def search(i: int):
    try:
      output_qtype = arolla_abc.infer_attr(op, tuple(input_qtypes)).qtype
    except ValueError:
      return
    if i < arity:
      for qtype in possible_qtypes_per_input[i]:
        input_qtypes[i] = qtype
        yield from search(i + 1)
      input_qtypes[i] = None
    else:
      assert output_qtype
      yield *input_qtypes[:arity], output_qtype

  yield from search(0)


def detect_qtype_signatures(
    op: arolla_abc.Operator,
    *,
    possible_qtypes: Iterable[
        arolla_abc.QType
    ] = DETECT_SIGNATURES_DEFAULT_QTYPES,
    max_arity: int | None = None,
) -> list[QTypeSignature]:
  """Returns a list of detected qtype signatures for the specified operator.

  IMPORTANT: Although this function typically has a complexity of

    O(len(result) * len(possible_qtypes) * max_arity),

  its worst-case complexity can still be exponential:

    O(len(possible_qtypes) ** max_arity).

  Args:
    op: An operator.
    possible_qtypes: A set of possible qtypes.
    max_arity: The maximum arity to attempt for the operator; by default, it's
      determined from the operator's signature. For variadic operators, this
      parameter is compulsory.

  Returns:
    Tuples with types: (arg_0_qtype, ..., return_qtype)
  """
  possible_qtypes = tuple(dict.fromkeys(possible_qtypes))
  # Check the signature assumptions.
  signature = arolla_abc.get_operator_signature(op)
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
  if var_positional_param is None:
    if max_arity is None:
      max_arity = len(signature.parameters)
    else:
      max_arity = min(max_arity, len(signature.parameters))
  elif max_arity is None:
    raise ValueError(
        'operator has a VAR_POSITIONAL parameter, please specify `max_arity`'
    )

  # Detect qtype signatures of arity up to max_arity.
  result = []
  for arity in range(max_arity + 1):
    result.extend(
        _detect_qtype_signatures_with_fixed_arity(
            op, possible_qtypes, signature, arity
        )
    )
  if not result:
    raise ValueError('found no supported qtype signatures')
  return list(map(tuple, result))


def assert_qtype_signatures_equal(
    actual_signatures: Iterable[QTypeSignature],
    expected_signatures: Iterable[QTypeSignature],
    *,
    max_errors_to_report: int | None = 10,
    msg: str | None = None,
):
  """Asserts that the given sets of signatures match.

  Args:
    actual_signatures: Actual signatures of the operator.
    expected_signatures: Expected signatures of the operator.
    max_errors_to_report: Limit of the number of inconsistent signatures to
      report per section. Set to None to report all the inconsistencies.
    msg: error message to override the default one.
  """
  # Keep only unique signatures preserving the original order.
  actual_signatures = tuple(dict.fromkeys(actual_signatures))
  expected_signatures = tuple(dict.fromkeys(expected_signatures))

  actual_output_qtypes = {s[:-1]: s[-1] for s in actual_signatures}
  expected_output_qtypes = {s[:-1]: s[-1] for s in expected_signatures}

  # NOTE: These errors probably indicate a tooling error instead of a test error
  # so a verbose error message here might never be needed.
  assert len(actual_signatures) == len(
      actual_output_qtypes
  ), 'duplicate input types found in actual signatures'
  assert len(expected_signatures) == len(
      expected_output_qtypes
  ), 'duplicate input types found in expected signatures'

  if max_errors_to_report is None:
    max_errors_to_report = len(actual_output_qtypes) + len(
        expected_output_qtypes
    )

  unsupported_args = list()
  unexpected_output_qtypes = list()
  for arg_qtypes, expected_output_qtype in expected_output_qtypes.items():
    if arg_qtypes not in actual_output_qtypes:
      unsupported_args.append(arg_qtypes)
    elif actual_output_qtypes[arg_qtypes] != expected_output_qtype:
      unexpected_output_qtypes.append(arg_qtypes)

  unexpected_supported_args = list()
  for arg_qtypes in actual_output_qtypes.keys():
    if arg_qtypes not in expected_output_qtypes:
      unexpected_supported_args.append(arg_qtypes)

  if (
      not unsupported_args
      and not unexpected_output_qtypes
      and not unexpected_supported_args
  ):
    return

  if msg is not None:
    raise AssertionError(msg)

  msg_lines = ['Sets of qtype signatures are not equal:']
  if unexpected_output_qtypes:
    msg_lines.append('')
    msg_lines.append('  Unexpected result qtypes:')
    for arg_qtypes in unexpected_output_qtypes[:max_errors_to_report]:
      msg_lines.append(
          f'    {arg_qtypes} -> {actual_output_qtypes[arg_qtypes]}, expected'
          f' {expected_output_qtypes[arg_qtypes]}'
      )
    if max_errors_to_report < len(unexpected_output_qtypes):
      msg_lines.append(
          '    ...'
          f' ({len(unexpected_output_qtypes) - max_errors_to_report} more)'
      )

  if unsupported_args:
    msg_lines.append('')
    msg_lines.append(
        '  The following qtype signatures expected, but not supported:'
    )
    for arg_qtypes in unsupported_args[:max_errors_to_report]:
      msg_lines.append(
          f'    {arg_qtypes} -> {expected_output_qtypes[arg_qtypes]}'
      )
    if max_errors_to_report < len(unsupported_args):
      msg_lines.append(
          f'    ... ({len(unsupported_args) - max_errors_to_report} more)'
      )

  if unexpected_supported_args:
    msg_lines.append('')
    msg_lines.append(
        '  The following qtype signatures supported, but not expected:'
    )
    for arg_qtypes in unexpected_supported_args[:max_errors_to_report]:
      msg_lines.append(
          f'    {arg_qtypes} -> {actual_output_qtypes[arg_qtypes]}'
      )
    if max_errors_to_report < len(unexpected_supported_args):
      msg_lines.append(
          '    ...'
          f' ({len(unexpected_supported_args) - max_errors_to_report} more)'
      )

  raise AssertionError('\n'.join(msg_lines))


def assert_qtype_signatures(
    op: arolla_abc.Operator,
    expected_signatures: Iterable[QTypeSignature],
    *,
    max_errors_to_report: int | None = 10,
    msg: str | None = None,
    possible_qtypes: Iterable[
        arolla_abc.QType
    ] = DETECT_SIGNATURES_DEFAULT_QTYPES,
    max_arity: int | None = None,
):
  """Detects signatures of the operator and asserts that they match the expected.

  Args:
    op: Operator to detect signatures for.
    expected_signatures: Expected signatures of the operator.
    max_errors_to_report: Limit of the number of inconsistent signatures to
      report per section. Set to None to report all the inconsistencies.
    msg: error message to override the default one.
    possible_qtypes: A set of possible qtypes.
    max_arity: The maximum arity to attempt for the operator; by default, it's
      determined from the operator's signature. For variadic operators, this
      parameter is compulsory.
  """
  actual_signatures = detect_qtype_signatures(
      op, possible_qtypes=possible_qtypes, max_arity=max_arity
  )
  expected_signatures = list(expected_signatures)
  assert_qtype_signatures_equal(
      actual_signatures,
      expected_signatures,
      max_errors_to_report=max_errors_to_report,
      msg=msg,
  )
