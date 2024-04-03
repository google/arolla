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

"""Basic facilities for arolla.abc.Signature."""

import inspect
from typing import Any, Callable

from arolla.abc import clib
from arolla.abc import qtype as abc_qtype
from arolla.abc import utils as abc_utils


# A cross-language-compatible representation of ExprOperatorSignature.
#
# class Signature:
#
#   Methods defined here:
#
#     __new__(cls, sequence)
#       Constructs an instance of the class.
#
#     __len__(self)
#       returns 2.
#
#     __getitem__(self, i)
#       returns i-th field value.
#
#   Data descriptors defined here:
#
#     parameters
#       (tuple[SignatureParameter, ...]) A list of parameters.
#
#     aux_policy
#       (str) An auxiliary policy (see ExprOperatorSignature for additional
#       information).
#
# (implementation: py/arolla/abc/py_signature.cc)
#
Signature = clib.Signature


# Type for arolla.abc.Signature parameters.
#
# class SignatureParameter:
#
#   Methods defined here:
#
#     __new__(cls, sequence)
#       Constructs an instance of the class.
#
#     __len__(self)
#       returns 3.
#
#     __getitem__(self, i)
#       returns i-th field value.
#
#   Data descriptors defined here:
#
#     name
#       (str) Parameter name.
#
#     kind
#       (str) Specifies how the arguments to the parameter: 'positional-only',
#       'positional-or-keyword', 'variadic-positional', 'keyword-only',
#       'variadic-keyword'."
#
#     default
#       (QValue|None) The default value for the parameter.
#
# (implementation: py/arolla/abc/py_signature.cc)
#
SignatureParameter = clib.SignatureParameter


# Returns the operator's signature.
get_operator_signature = clib.get_operator_signature

# Type annotation for make_operator_signature() argument.
#
# It covers the following argument types:
#
# (0) Signature
# (1) A string like: 'x, y, z, *args'
# (2) A tuple like: ('x, y=, z=, *args', default_y, default_z)
# (3) inspect.Signature
#
# You can find additional information in the documentation
# for make_operator_signature().
MakeOperatorSignatureArg = Signature | str | tuple[Any, ...] | inspect.Signature


def _as_qvalue(value) -> abc_qtype.QValue:
  if isinstance(value, abc_qtype.QValue):
    return value
  raise TypeError(
      f'unable to represent ({abc_utils.get_type_name(type(value))})'
      f' {value!r} as QValue; you may try to specify a custom `as_qvalue` for'
      ' arolla.abc.make_operator_signature()'
  )


# A mapping of `inspect.Signature` parameter kinds supported by
# `arolla::expr::ExprOperatorSignature`.
_MAPPING_FROM_INSPECT_PARAM_KIND = {
    inspect.Parameter.POSITIONAL_OR_KEYWORD: 'positional-or-keyword',
    inspect.Parameter.VAR_POSITIONAL: 'variadic-positional',
}


# A mapping of `arolla.abc.Signature` parameter kinds supported by
# `inspect.Signature`.
_MAPPING_TO_INSPECT_PARAM_KIND = {
    'positional-only': inspect.Parameter.POSITIONAL_ONLY,
    'positional-or-keyword': inspect.Parameter.POSITIONAL_OR_KEYWORD,
    'variadic-positional': inspect.Parameter.VAR_POSITIONAL,
    'keyword-only': inspect.Parameter.KEYWORD_ONLY,
    'variadic-keyword': inspect.Parameter.VAR_KEYWORD,
}


def make_operator_signature(
    signature: MakeOperatorSignatureArg,
    as_qvalue: Callable[[Any], abc_qtype.QValue] = _as_qvalue,
) -> Signature:
  """Returns a signature instance.

  There are three main ways to define a signature:

  (1) A string with comma separated parameter names.

      Example:

        'x, y, z, *args'

      -- defines three keyword-or-positional parameters `x`, `y`, `z` and
      a variadic-positional parameter `args`.

  (2) A tuple instance (or a list)

        (signature_spec, value1, value2, ..., valuen)

      where `signature_spec` is a string with comma separated parameter names,
      like in (1), with an addition that some parameters may have a trailing
      character '='. It means that the corresponding parameter has a default
      value, that is specified further in the tuple.

      Example:

        ('x, y=, z=, *args', default_y, default_z)

      -- defines three keyword-or-positional parameters `x`, `y`, `z` and
      a variadic-positional parameter `args`. Parameters `y` and `z` have
      default values `default_y` and `default_z` respectively.

  (3) inspect.Signature instance.

  Args:
    signature: A signature definition.
    as_qvalue: (optional) An auto-boxing function for the default values; takes
      a python value, and returns a qvalue.

  Returns:
    An instance of `arolla.abc.Signature`.
  """
  if isinstance(signature, Signature):
    return signature

  if isinstance(signature, str):
    return clib.internal_make_operator_signature(signature, ())

  if isinstance(signature, tuple):
    return clib.internal_make_operator_signature(
        signature[0], list(map(as_qvalue, signature[1:]))
    )

  if isinstance(signature, inspect.Signature):
    params = []
    for param in signature.parameters.values():
      param_kind = _MAPPING_FROM_INSPECT_PARAM_KIND.get(param.kind)
      if not param_kind:
        raise ValueError(
            f'unsupported parameter kind: {param.name=!r},'
            f' param.kind={param.kind.name}'
        )
      param_default = None
      if param.default is not inspect.Parameter.empty:
        param_default = as_qvalue(param.default)
      params.append(SignatureParameter((param.name, param_kind, param_default)))
    return Signature((tuple(params), ''))

  raise TypeError(
      'expected str, tuple, or inspect.Signature, got'
      f' signature: {abc_utils.get_type_name(type(signature))}'
  )


def make_inspect_signature(signature: Signature) -> inspect.Signature:
  """Converts arolla.abc.Signature to inspect.Signature."""
  inspect_params = []
  for param in signature.parameters:
    inspect_param_kind = _MAPPING_TO_INSPECT_PARAM_KIND.get(param.kind)
    if inspect_param_kind is None:
      raise ValueError(
          f'unexpected parameter kind: {param.name=!r}, {param.kind=!r}'
      )
    default = param.default
    inspect_params.append(
        inspect.Parameter(
            param.name,
            inspect_param_kind,
            default=inspect.Parameter.empty if default is None else default,
        )
    )
  return inspect.Signature(inspect_params)
