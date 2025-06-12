# Copyright 2025 Google LLC
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

"""Declaration of M.strings.* operators."""

from arolla import arolla
from arolla.operators.standard import core as M_core
from arolla.operators.standard import qtype as M_qtype
from arolla.operators.standard import seq as M_seq

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

# Bootstrap operators:
# go/keep-sorted start
static_decode = arolla.abc.lookup_operator('strings.static_decode')
# go/keep-sorted end


def _lift_dynamically(op):
  """Extends a scalar backend operator to support arrays dynamically.

  This is an alternative to lift_to_array/lift_to_dense_array in BUILD file. The
  advantages over them are:

    - Support of variadic arguments.
    - Smaller binary size.
    - Support for mix of scalars and arrays, and so no broadcasting in runtime.

  While the main disadvantage is a way slower execution due to runtime overhead
  of core.map operator.

  Args:
    op: operator to lift.

  Returns:
    lifted operator.
  """

  @arolla.optools.as_lambda_operator(
      f'{op.display_name}.lifted',
      qtype_constraints=[(
          M_seq.reduce(
              M_qtype.broadcast_qtype_like,
              M_qtype.get_field_qtypes(P.args),
              arolla.UNIT,
          )
          != arolla.NOTHING,
          'unused',
      )],
  )
  def lifted(*args):
    args = arolla.optools.fix_trace_args(args)
    return M_core.apply_varargs(M_core.map_, op, args)

  return arolla.types.OverloadedOperator(op, lifted, name=op.display_name)


# pylint: disable=unused-argument


@arolla.optools.add_to_registry()
@_lift_dynamically
@arolla.optools.as_backend_operator(
    'strings.contains',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.substr),
        constraints.expect_scalar_or_optional(P.substr),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(
        M_qtype.common_qtype(P.s, P.substr), arolla.OPTIONAL_UNIT
    ),
)
def contains(s, substr):
  """Returns present iff `s` contains `substr`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.count',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_array_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.substr),
        constraints.expect_array_scalar_or_optional(P.substr),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(
        M_qtype.common_qtype(P.s, P.substr), arolla.INT64
    ),
)
def count(s, substr):
  """Counts the number of occurrences of `substr` in `s`."""
  raise NotImplementedError('provided by backend')


def _parse_value(result_qtype: arolla.QType):
  """Returns constraints and type inference for parse_* operations."""
  return dict(
      qtype_constraints=[constraints.expect_texts_or_byteses(P.x)],
      qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, result_qtype),
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.parse_int32', **_parse_value(arolla.INT32)
)
def parse_int32(x):
  """Converts Bytes/Text to int32."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.parse_int64', **_parse_value(arolla.INT64)
)
def parse_int64(x):
  """Converts Bytes/Text to int64."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.parse_float32', **_parse_value(arolla.FLOAT32)
)
def parse_float32(x):
  """Converts Bytes/Text to float32."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.parse_float64', **_parse_value(arolla.FLOAT64)
)
def parse_float64(x):
  """Converts Bytes/Text to float64."""
  raise NotImplementedError('provided by backend')


_unary_texts = dict(
    qtype_constraints=[constraints.expect_texts(P.x)],
    qtype_inference_expr=P.x,
)


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('strings.lower', **_unary_texts)
def lower(x):
  """Converts uppercase characters into their lowercase replacements."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator('strings.upper', **_unary_texts)
def upper(x):
  """Converts lowercase characters into their uppercase replacements."""
  raise NotImplementedError('provided by backend')


# TODO: Support `encoding` argument (except the default UTF-8).
@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.encode',
    qtype_constraints=[constraints.expect_texts(P.x)],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.BYTES),
)
def encode(x):
  """Encodes text to bytes element-wise (using utf-8 coding)."""
  raise NotImplementedError('provided by backend')


# TODO: Support `encoding` argument (except the default UTF-8).
@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.decode',
    qtype_constraints=[constraints.expect_byteses(P.x)],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.TEXT),
)
def decode(x):
  """Decodes bytes to text element-wise (using utf-8 coding)."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.as_text',
    qtype_constraints=[constraints.has_scalar_type(P.x)],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.TEXT),
)
def as_text(x):
  """Converts each element in the given value to text."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.find',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_array_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.substr),
        constraints.expect_array_scalar_or_optional(P.substr),
        constraints.expect_implicit_cast_compatible(P.s, P.substr),
        constraints.expect_integers(P.start),
        constraints.expect_array_scalar_or_optional(P.start),
        constraints.expect_integers(P.end),
        constraints.expect_array_scalar_or_optional(P.end),
    ],
    qtype_inference_expr=constraints.broadcast_qtype_expr(
        [P.s, P.substr, P.start, P.end], arolla.OPTIONAL_INT64
    ),
)
def find(s, substr, start=0, end=arolla.optional_int64(None)):
  """Returns the offset of the first occurrence of `substr` in `s`.

  The units of `start`, `end`, and the return value are all byte offsets if `s`
  is `Bytes` and codepoint offsets if `s` is `Text`.

  Args:
   s: (Text or Bytes) String to search in.
   substr: (Text or Bytes) String to search for.
   start: (optional int) Offset to start the search, defaults to 0.
   end: (optional int) Offset to stop the search, defaults to end of the string.

  Returns:
    The offset of the first `substr` occurrence, or missing if there are no
    occurrences.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.rfind',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_array_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.substr),
        constraints.expect_array_scalar_or_optional(P.substr),
        constraints.expect_implicit_cast_compatible(P.s, P.substr),
        constraints.expect_integers(P.start),
        constraints.expect_array_scalar_or_optional(P.start),
        constraints.expect_integers(P.end),
        constraints.expect_array_scalar_or_optional(P.end),
    ],
    qtype_inference_expr=constraints.broadcast_qtype_expr(
        [P.s, P.substr, P.start, P.end], arolla.OPTIONAL_INT64
    ),
)
def rfind(s, substr, start=0, end=arolla.optional_int64(None)):
  """Returns the offset of the last occurrence of `substr` in `s`.

  The units of `start`, `end`, and the return value are all byte offsets if `s`
  is `Bytes` and codepoint offsets if `s` is `Text`.

  Args:
   s: (Text or Bytes) String to search in.
   substr: (Text or Bytes) String to search for.
   start: (optional int) Offset to start the search, defaults to 0.
   end: (optional int) Offset to stop the search, defaults to end of the string.

  Returns:
    The offset of the last `substr` occurrence, or missing if there are no
    occurrences.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.as_lambda_operator(
    'strings._printf_bytes._is_supported_by_scalar_printf_bytes'
)
def _is_supported_by_scalar_printf_bytes(t):
  return (M_qtype.is_scalar_qtype(t) | M_qtype.is_optional_qtype(t)) & (
      M_qtype.is_numeric_qtype(t)
      | (M_qtype.get_scalar_qtype(t) == arolla.BOOLEAN)
      | (M_qtype.get_scalar_qtype(t) == arolla.BYTES)
  )


def _args_for_formatting_constraint(args):
  return (
      M_seq.all_(
          M_seq.map_(
              _is_supported_by_scalar_printf_bytes,
              M_qtype.get_field_qtypes(args),
          )
      ),
      (
          'expected arguments to contain numerics, BYTES or'
          f' BOOLEAN, got {constraints.variadic_name_type_msg(args)}'
      ),
  )


@arolla.optools.add_to_registry()
@_lift_dynamically
@arolla.optools.as_backend_operator(
    'strings._printf_bytes',
    qtype_constraints=[
        constraints.expect_scalar_or_optional(P.fmt),
        constraints.expect_byteses(P.fmt),
        _args_for_formatting_constraint(P.args),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(
        M_seq.reduce(
            M_qtype.broadcast_qtype_like,
            M_qtype.get_field_qtypes(P.args),
            P.fmt,
        ),
        arolla.BYTES,
    ),
)
def _printf_bytes(fmt, *args):
  """(Internal) strings.printf version on BYTES."""
  raise NotImplementedError('provided by backend')


@arolla.optools.as_lambda_operator(
    'strings.printf._identity_if_not_bytes',
    qtype_constraints=[
        (M_qtype.get_scalar_qtype(P.x) != arolla.BYTES, 'unused')
    ],
)
def _identity_if_not_bytes(x):
  return x


@arolla.optools.as_lambda_operator('strings.printf._format_text')
def _printf_text(fmt, *args):
  args = arolla.optools.fix_trace_args(args)
  text_to_bytes = arolla.optools.dispatch[encode, _identity_if_not_bytes]
  return decode(
      M_core.apply_varargs(
          _printf_bytes, encode(fmt), M_core.map_tuple(text_to_bytes, args)
      )
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.printf',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.fmt),
        (
            M_seq.reduce(
                M_qtype.broadcast_qtype_like,
                M_qtype.get_field_qtypes(P.args),
                P.fmt,
            )
            != arolla.NOTHING,
            (
                'all printf args must be broadcast compatible, got'
                f' {constraints.name_type_msg(P.fmt)},'
                f' {constraints.name_type_msg(P.args)}'
            ),
        ),
    ],
)
def printf(fmt, *args):
  """Formats string(s) according to printf-style format string(s).

  See absl::StrFormat documentation for the format string details.

  Args:
    fmt: format string, TEXT or BYTES.
    *args: arguments to format.

  Returns:
    Formatted TEXT or BYTES.
  """
  args = arolla.optools.fix_trace_args(args)
  return M_core.apply_varargs(
      arolla.optools.dispatch[_printf_text, _printf_bytes], fmt, args
  )


@arolla.optools.add_to_registry()
@_lift_dynamically
@arolla.optools.as_backend_operator(
    'strings._format_bytes',
    qtype_constraints=[
        constraints.expect_scalar_or_optional(P.fmt),
        constraints.expect_byteses(P.fmt),
        constraints.expect_scalar_text(P.arg_names),
        _args_for_formatting_constraint(P.args),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(
        M_seq.reduce(
            M_qtype.broadcast_qtype_like,
            M_qtype.get_field_qtypes(P.args),
            P.fmt,
        ),
        arolla.BYTES,
    ),
)
def _format_bytes(fmt, arg_names, *args):
  """(Internal) strings.format version on BYTES."""
  raise NotImplementedError('provided by backend')


@arolla.optools.as_lambda_operator('strings.format._format_text')
def _format_text(fmt, arg_names, *args):
  args = arolla.optools.fix_trace_args(args)
  text_to_bytes = arolla.optools.dispatch[encode, _identity_if_not_bytes]
  return decode(
      M_core.apply_varargs(
          _format_bytes,
          encode(fmt),
          arg_names,
          M_core.map_tuple(text_to_bytes, args),
      )
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.format',
    qtype_constraints=[
        constraints.expect_scalar_text(P.arg_names),
    ],
    experimental_aux_policy='experimental_format_args',
)
def format_(fmt, arg_names, *kwargs):  # pylint: disable=g-doc-args
  """Formats according to Python `str.format` style.

  Format support is slightly different from Python:
  1. {x:v} is equivalent to {x} and supported for all types as default string
     format.
  2. Only float and integers support other format specifiers.
    E.g., {x:.1f} and {x:04d}.
  3. If format is missing type specifier `f` or `d` at the end, we are
     adding it automatically based on the type of the argument.

  Note: only keyword arguments are supported.

  Examples:
    M.strings.format('Hello {n}!', n='World')
      # -> 'Hello World!'
    M.strings.format('{a} + {b} = {c}', a=1, b=2, c=3)
      # -> '1 + 2 = 3'
    M.strings.format('{a} + {b} = {c}', a=[1, 3], b=[2, 1], c=[3, 4])
      # -> ['1 + 2 = 3', '3 + 1 = 4']

  Examples with format specifiers:
    M.strings.format(
        '({a:03} + {b:e}) * {c:.2f} ='
        ' {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}'
        a=5, b=5.7, c=75)
      # -> '(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000'

  Args:
    fmt: format string, TEXT or BYTES.
    *kwargs: arguments to format.

  Returns:
    Formatted TEXT or BYTES.
  """
  kwargs = arolla.optools.fix_trace_args(kwargs)
  return M_core.apply_varargs(
      arolla.optools.dispatch[_format_text, _format_bytes],
      fmt,
      arg_names,
      kwargs,
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.length',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.x),
    ],
    qtype_inference_expr=M_qtype.broadcast_qtype_like(P.x, arolla.INT64),
)
def length(x):
  """Returns length of a string in bytes (for Byte) or codepoints (for Text)."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.replace',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_array_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.old),
        constraints.expect_array_scalar_or_optional(P.old),
        constraints.expect_texts_or_byteses(P.new),
        constraints.expect_array_scalar_or_optional(P.new),
        constraints.expect_integers(P.max_subs),
        constraints.expect_array_scalar_or_optional(P.max_subs),
    ],
    qtype_inference_expr=M_qtype.common_qtype(
        P.s, M_qtype.common_qtype(P.old, P.new)
    ),
)
def replace(s, old, new, max_subs=arolla.optional_int64(None)):
  """Replaces up to `max_subs` occurrences of `old` within `s` with `new`.

  If `max_subs` is missing or negative, then there is no limit on the number of
  substitutions. If it is zero, then `s` is returned unchanged.

  If the search string is empty, the original string is fenced with the
  replacement string, for example: replace("ab", "", "-") returns "-a-b-". That
  behavior is similar to Python's string replace.

  Args:
   s: (Text or Bytes) Original string.
   old: (Text or Bytes) String to replace.
   new: (Text or Bytes) Replacement string.
   max_subs: (optional int) Max number of substitutions. If unspecified or
     negative, then there is no limit on the number of substitutions.

  Returns:
    String with applied substitutions.
  """
  raise NotImplementedError('provided by backend')


# TODO: Replace with DispatchOperator where possible.
def _default_empty_string(x):
  """Returns an empty Bytes or Text depending on x."""
  empty_text = arolla.types.RestrictedLambdaOperator(
      'unused_x',
      arolla.text(''),
      qtype_constraints=[constraints.expect_texts(P.unused_x)],
  )
  empty_bytes = arolla.types.RestrictedLambdaOperator(
      'unused_x',
      arolla.bytes(b''),
      qtype_constraints=[constraints.expect_byteses(P.unused_x)],
  )
  return arolla.optools.dispatch[empty_text, empty_bytes](x)


def _default_missing_string(x):
  """Returns a missing Optional Bytes or Text depending on x."""
  missing_text = arolla.types.RestrictedLambdaOperator(
      'unused_x',
      arolla.optional_text(None),
      qtype_constraints=[constraints.expect_texts(P.unused_x)],
  )
  missing_bytes = arolla.types.RestrictedLambdaOperator(
      'unused_x',
      arolla.optional_bytes(None),
      qtype_constraints=[constraints.expect_byteses(P.unused_x)],
  )
  return arolla.optools.dispatch[missing_text, missing_bytes](x)


@_lift_dynamically
@arolla.optools.as_backend_operator(
    'strings._lstrip',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.chars),
        constraints.expect_scalar_or_optional(P.chars),
        (
            M.qtype.get_scalar_qtype(P.s) == M.qtype.get_scalar_qtype(P.chars),
            (
                'expected arguments to be of the same scalar qtype, got'
                f' {constraints.name_type_msg(P.s)} and'
                f' {constraints.name_type_msg(P.chars)}'
            ),
        ),
    ],
    qtype_inference_expr=P.s,
)
def _lstrip(s, chars):
  """Expr proxy for strings._lstrip backend operator."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.lstrip',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        (
            (M.qtype.get_scalar_qtype(P.chars) == arolla.BYTES)
            | (M.qtype.get_scalar_qtype(P.chars) == arolla.TEXT)
            | (P.chars == arolla.UNSPECIFIED),
            (
                'expected `chars` to be bytes, text, or unspecified, got'
                f' {constraints.name_type_msg(P.chars)}'
            ),
        ),
    ],
)
def lstrip(s, chars=arolla.unspecified()):
  """Strips whitespaces or the specified characters from front of s."""
  chars = M.core.default_if_unspecified(chars, _default_missing_string(s))
  return _lstrip(s, chars)


@_lift_dynamically
@arolla.optools.as_backend_operator(
    'strings._rstrip',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.chars),
        constraints.expect_scalar_or_optional(P.chars),
        (
            M.qtype.get_scalar_qtype(P.s) == M.qtype.get_scalar_qtype(P.chars),
            (
                'expected arguments to be of the same scalar qtype, got'
                f' {constraints.name_type_msg(P.s)} and'
                f' {constraints.name_type_msg(P.chars)}'
            ),
        ),
    ],
    qtype_inference_expr=P.s,
)
def _rstrip(s, chars):
  """Expr proxy for strings._rstrip backend operator."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.rstrip',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        (
            (M.qtype.get_scalar_qtype(P.chars) == arolla.BYTES)
            | (M.qtype.get_scalar_qtype(P.chars) == arolla.TEXT)
            | (P.chars == arolla.UNSPECIFIED),
            (
                'expected `chars` to be bytes, text, or unspecified, got'
                f' {constraints.name_type_msg(P.chars)}'
            ),
        ),
    ],
)
def rstrip(s, chars=arolla.unspecified()):
  """Strips whitespaces or the specified characters from back of s."""
  chars = M.core.default_if_unspecified(chars, _default_missing_string(s))
  return _rstrip(s, chars)


@_lift_dynamically
@arolla.optools.as_backend_operator(
    'strings._strip',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_scalar_or_optional(P.s),
        constraints.expect_texts_or_byteses(P.chars),
        constraints.expect_scalar_or_optional(P.chars),
        (
            M.qtype.get_scalar_qtype(P.s) == M.qtype.get_scalar_qtype(P.chars),
            (
                'Expected arguments to be of the same scalar qtype, got'
                f' {constraints.name_type_msg(P.s)} and'
                f' {constraints.name_type_msg(P.chars)}'
            ),
        ),
    ],
    qtype_inference_expr=P.s,
)
def _strip(s, chars):
  """Expr proxy for strings._strip backend operator."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.strip',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        (
            (M.qtype.get_scalar_qtype(P.chars) == arolla.BYTES)
            | (M.qtype.get_scalar_qtype(P.chars) == arolla.TEXT)
            | (P.chars == arolla.UNSPECIFIED),
            (
                'expected `chars` to be bytes, text, or unspecified, got'
                f' {constraints.name_type_msg(P.chars)}'
            ),
        ),
    ],
)
def strip(s, chars=arolla.unspecified()):
  """Strips whitespaces or the specified characters from the end and front of s.

   If `chars` is missing, then whitespaces are removed.
   If `chars` is present, then the leading and tailing characters present in the
   `chars` set will be
    removed, up to the first and last character respectively in the string that
    are not present in `chars`.

  Args:
   s: (Text or Bytes) Original string.
   chars (Optional Text or Bytes): The set of chars to remove.

  Returns:
    Stripped string.
  """
  chars = M.core.default_if_unspecified(chars, _default_missing_string(s))
  return _strip(s, chars)


@arolla.optools.as_backend_operator(
    'strings._split',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_array(P.s),
        constraints.expect_texts_or_byteses(P.sep),
        constraints.expect_scalar_or_optional(P.sep),
        (
            M.qtype.get_scalar_qtype(P.s) == M.qtype.get_scalar_qtype(P.sep),
            (
                'Expected arguments to be of the same scalar qtype, got'
                f' {constraints.name_type_msg(P.s)} and'
                f' {constraints.name_type_msg(P.sep)}'
            ),
        ),
    ],
    qtype_inference_expr=M.qtype.make_tuple_qtype(
        P.s, M.qtype.get_edge_qtype(P.s)
    ),
)
def _split(s, sep):
  """Expr proxy for strings._split backend operator."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.split',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        (
            (M.qtype.get_scalar_qtype(P.sep) == arolla.BYTES)
            | (M.qtype.get_scalar_qtype(P.sep) == arolla.TEXT)
            | (P.sep == arolla.UNSPECIFIED),
            (
                'expected `sep` to be bytes, text, or unspecified, got'
                f' {constraints.name_type_msg(P.sep)}'
            ),
        ),
    ],
)
def split(s, sep=arolla.unspecified()):
  """Splits s into several parts with the specified separator.

   If `sep` is missing, will split by whitespaces, with empty strings ignored.
   If `sep` is present, then will split by occurrences of the given substring,
     with empty strings not ignored.

  Args:
   s: (Text or Bytes) Original string.
   sep (Optional Text or Bytes): The substring to separate by. By default will
     separate by whitespaces (where a sequence of whitespaces is considered as a
     single separator, or in other words ignoring empty strings)

  Returns:
    Tuple of two elements: the array of split substrings, and an edge mapping
    these substrings to the original string.
  """
  sep = M.core.default_if_unspecified(sep, _default_missing_string(s))
  return _split(s, sep)


def _strip_optional_qtype_expr(x):
  """Expr that transforms optional qtypes into scalars, but preserves arrays."""
  return M.qtype.conditional_qtype(
      M.qtype.is_optional_qtype(x), M.qtype.get_value_qtype(x), x
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings.substr',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.s),
        constraints.expect_array_scalar_or_optional(P.s),
        constraints.expect_integers(P.start),
        constraints.expect_array_scalar_or_optional(P.start),
        constraints.expect_integers(P.end),
        constraints.expect_array_scalar_or_optional(P.end),
    ],
    qtype_inference_expr=constraints.broadcast_qtype_expr(
        [
            _strip_optional_qtype_expr(P.start),
            _strip_optional_qtype_expr(P.end),
        ],
        P.s,
    ),
)
def substr_(s, start=0, end=arolla.optional_int64(None)):
  """Returns the substring of `s` over the range `[start, end)`.

  `start` and `end` represent byte offsets if `s` type is Bytes, and
  codepoint offsets if `s` type is `Text`.

  Args:
    s: (Text or Bytes) String to substring.
    start: (integer) Start offset, 0 if not specified.
    end: (integer) End offset, `len(s)` if not specified.

  Returns:
    (Text or Bytes) Substring.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings._agg_join',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.x),
        constraints.expect_array(P.x),
        *constraints.expect_edge(P.into, child_side_param=P.x),
        constraints.expect_texts_or_byteses(P.sep),
        constraints.expect_scalar(P.sep),
        (
            M_qtype.get_scalar_qtype(P.x) == P.sep,
            (
                'separator type should match array type, but got'
                f' {constraints.name_type_msg(P.sep)} and'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
    ],
    qtype_inference_expr=M_qtype.with_value_qtype(
        M_qtype.get_parent_shape_qtype(P.into), M_qtype.get_scalar_qtype(P.x)
    ),
)
def _agg_join(x, into, sep):
  """(Internal) Wrapper over backend strings._agg_join."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.agg_join',
    qtype_constraints=[
        constraints.expect_array(P.x),
        constraints.expect_texts_or_byteses(P.x),
        *constraints.expect_edge_or_unspecified(P.into, child_side_param=P.x),
    ],
)
def agg_join(x, into=arolla.unspecified(), sep=arolla.unspecified()):
  """Joins strings into an edge, separating items within a group with `sep`."""
  empty_string = _default_empty_string(x)
  sep = M.core.default_if_unspecified(sep, empty_string) | empty_string
  return _agg_join(
      x, M.core.default_if_unspecified(into, M.edge.to_single(x)), sep
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings._compile_regex',
    qtype_constraints=[
        constraints.expect_scalar_text(P.pattern),
    ],
    qtype_inference_expr=M_qtype.REGEX_QTYPE,
)
def _compile_regex(pattern):
  """(internal) Returns a compiled regular expression (RE2 syntax)."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings._contains_regex',
    qtype_constraints=[
        (
            (P.text == arolla.TEXT) | (P.text == arolla.OPTIONAL_TEXT),
            f'expected a text, got {constraints.name_type_msg(P.text)}',
        ),
        constraints.expect_regex(P.regex),
    ],
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
)
def _contains_regex(text, regex):
  """(internal) Returns `present` if `text` contains `regex`.

  Args:
    text: A text string.
    regex: A compiled regular expression.

  Returns:
    `present` if `text` contains a pattern represented by `regex`.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.contains_regex',
    qtype_constraints=[
        constraints.expect_texts(P.text),
        constraints.expect_scalar_text(P.regex),
    ],
)
def contains_regex(text, regex):
  """Returns `present` if `text` contains a pattern represented by `regex`.

  Args:
    text: A text string.
    regex: A text string with a regex (RE2 syntax).

  Returns:
    `present` if `text` contains a pattern represented by `regex`.
  """
  dispatch_op = arolla.types.DispatchOperator(
      'text, regex',
      scalar_case=arolla.types.DispatchCase(
          _contains_regex(P.text, P.regex),
          condition=(P.text == arolla.TEXT) | (P.text == arolla.OPTIONAL_TEXT),
      ),
      default=M_core.map_(_contains_regex, P.text, P.regex),
  )
  return dispatch_op(text, _compile_regex(regex))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings._extract_regex',
    qtype_constraints=[
        (
            (P.text == arolla.TEXT) | (P.text == arolla.OPTIONAL_TEXT),
            f'expected a text, got {constraints.name_type_msg(P.text)}',
        ),
        constraints.expect_regex(P.regex),
    ],
    qtype_inference_expr=arolla.OPTIONAL_TEXT,
)
def _extract_regex(text, regex):
  """(internal) Returns the match if `text` matches `regex`.

  Args:
    text: A text string.
    regex: A compiled regular expression; the expression should have exactly one
      capturing group.

  Returns:
    For the first partial match of `regex` and `text`, returns the substring of
    `text` that matches the capturing group of `regex`.
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.extract_regex',
    qtype_constraints=[
        constraints.expect_texts(P.text),
        constraints.expect_scalar_text(P.regex),
    ],
)
def extract_regex(text, regex):
  """Returns the match if `text` matches a pattern represented by `regex`.

  Args:
    text: A text string.
    regex: A text string with a regex (RE2 syntax); the expression should have
      exactly one capturing group.

  Returns:
    For the first partial match of `regex` and `text`, returns the substring of
    `text` that matches the capturing group of `regex`.
  """
  dispatch_op = arolla.types.DispatchOperator(
      'text, regex',
      scalar_case=arolla.types.DispatchCase(
          _extract_regex(P.text, P.regex),
          condition=(P.text == arolla.TEXT) | (P.text == arolla.OPTIONAL_TEXT),
      ),
      default=M_core.map_(_extract_regex, P.text, P.regex),
  )
  return dispatch_op(text, _compile_regex(regex))


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'strings._replace_all_regex',
    qtype_constraints=[
        (
            (P.text == arolla.TEXT) | (P.text == arolla.OPTIONAL_TEXT),
            f'expected a text, got {constraints.name_type_msg(P.text)}',
        ),
        constraints.expect_regex(P.regex),
        (
            (P.replacement == arolla.TEXT)
            | (P.replacement == arolla.OPTIONAL_TEXT),
            f'expected a text, got {constraints.name_type_msg(P.replacement)}',
        ),
    ],
    qtype_inference_expr=arolla.OPTIONAL_TEXT,
)
def _replace_all_regex(text, regex, replacement):
  """(internal) Replaces all non-overlapping matches of `regex` in `text`."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.replace_all_regex',
    qtype_constraints=[
        constraints.expect_texts(P.text),
        constraints.expect_scalar_text(P.regex),
        constraints.expect_texts(P.replacement),
    ],
)
def replace_all_regex(text, regex, replacement):
  r"""Replaces all non-overlapping matches of `regex` in `text`.

  Examples:
    * strings.replace_all_regex('banana', 'ana', 'ono')  # -> 'bonona'
    * strings.replace_all_regex('banama', 'a(.)', r'o\1a')  # -> 'bonaomaa'
    * strings.replace_all_regex(
        array([None, 'banana', 'apple']),
        'a(.)',
        array(['foo', r'o\1p', 'baz']),
      )  # -> array([None, 'bonponpa', 'bazple']),

  Args:
    text: A text string.
    regex: A text string that represents a regex (RE2 syntax).
    replacement: A text string that should replace each match. Backslash-escaped
      digits (\1 to \9) can be used to reference the text that matched the
      corresponding capturing group from the pattern, while \0 refers to the
      entire match. Replacements are not subject to re-matching. Since it only
      replaces non-overlapping matches, replacing "ana" within "banana" makes
      only one replacement, not two.

  Returns:
    The text string where the replacements have been made.
  """
  dispatch_op = arolla.types.DispatchOperator(
      'text, regex, replacement',
      scalar_case=arolla.types.DispatchCase(
          _replace_all_regex(P.text, P.regex, P.replacement),
          condition=(
              (
                  M_qtype.is_scalar_qtype(P.text)
                  | M_qtype.is_optional_qtype(P.text)
              )
              & (
                  M_qtype.is_scalar_qtype(P.replacement)
                  | M_qtype.is_optional_qtype(P.replacement)
              )
          ),
      ),
      default=M_core.map_(_replace_all_regex, P.text, P.regex, P.replacement),
  )
  return dispatch_op(text, _compile_regex(regex), replacement)


@arolla.optools.as_backend_operator(
    'strings._findall_regex',
    qtype_constraints=[
        constraints.expect_texts(P.s),
        constraints.expect_array(P.s),
        constraints.expect_regex(P.compiled_regex),
    ],
    qtype_inference_expr=M.qtype.make_tuple_qtype(
        P.s, M.qtype.get_edge_qtype(P.s), M.qtype.get_edge_qtype(P.s)
    ),
)
def _findall_regex(s, compiled_regex):
  """Expr proxy for strings._findall_regex backend operator."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.findall_regex',
    qtype_constraints=[
        constraints.expect_texts(P.s),
        constraints.expect_scalar_text(P.regex),
    ],
)
def findall_regex(s, regex):
  r"""Returns the submatches of all non-overlapping matches of `regex` in `s`.

  The strings in `s` are scanned left-to-right to find all non-overlapping
  matches of `regex`. The order of the matches is preserved in the result. For
  each match, the substring matched by each capturing group of `regex` is
  recorded. The result contains these substrings, plus an edge to indicate which
  element of `s` was matched, plus an edge to indicate which capturing group in
  `regex` was matched.

  Example:
    x = array([
        "I had 5 bottles of beer tonight",
        None,
        "foo",
        "100 bottles of beer, no, 1000 bottles of beer",
    ])
    strings.findall_regex(x, "(\\d+) (bottles) (of) beer")
    #  -> tuple(
    #         array([
    #             "5",
    #             "bottles",
    #             "of",
    #             "100",
    #             "bottles",
    #             "of",
    #             "1000",
    #             "bottles",
    #             "of",
    #         ]),
    #         edge.from_sizes([1, 0, 0, 2]),
    #         edge.from_sizes([3, 3, 3]),
    #     )

  Args:
    s: A text string.
    regex: A text string that represents a regex (RE2 syntax).

  Returns:
    A tuple of three elements:
    - An array of strings, where each element is the substring matched by a
      capturing group in `regex`.
    - An edge mapping each element of the result to the original string. I.e.
      the edge indicates which element of the array of strings `s` was matched.
    - An edge mapping each element of the result to the capturing groups. I.e.
      it indicates the capturing group in the `regex` that was matched.
  """

  def _findall_regex_scalar_case(s, compiled_regex):
    s_array = M.array.as_dense_array(s)
    result_tuple = _findall_regex(s_array, compiled_regex)
    matches = M.core.get_nth(result_tuple, 0)
    matches_edge = M.core.get_nth(result_tuple, 1)
    groups_edge = M.core.get_nth(result_tuple, 2)
    return M.core.make_tuple(
        matches,
        M.edge.from_shape(
            M.array.make_dense_array_shape(M.edge.child_size(matches_edge))
        ),
        groups_edge,
    )

  dispatch_op = arolla.types.DispatchOperator(
      's, regex',
      scalar_case=arolla.types.DispatchCase(
          _findall_regex_scalar_case(P.s, P.regex),
          condition=(P.s == arolla.TEXT) | (P.s == arolla.OPTIONAL_TEXT),
      ),
      default=_findall_regex(P.s, P.regex),
  )
  return dispatch_op(s, _compile_regex(regex))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.join_with_separator',
    qtype_constraints=[
        (
            (P.sep == arolla.TEXT) | (P.sep == arolla.BYTES),
            (
                'expected a scalar text or bytes, got'
                f' {constraints.name_type_msg(P.sep)}'
            ),
        ),
        (
            (
                M.qtype.get_scalar_qtype(
                    M_seq.reduce(
                        M_qtype.common_qtype,
                        M_qtype.get_field_qtypes(P.args),
                        P.arg0,
                    )
                )
                == P.sep
            ),
            (
                'expected all arguments to have compatible string types, got'
                f' {constraints.name_type_msg(P.sep)},'
                f' {constraints.name_type_msg(P.arg0)},'
                f' {constraints.variadic_name_type_msg(P.args)}'
            ),
        ),
    ],
)
def join_with_separator(sep, arg0, *args):
  """Concatenates the arguments, interleaving them with `sep`."""
  args = arolla.optools.fix_trace_args(args)

  @arolla.optools.as_backend_operator(
      'strings._join_with_separator',
      qtype_constraints=[
          (
              ~M_qtype.is_array_qtype(
                  M_seq.reduce(
                      M_qtype.common_qtype,
                      M_qtype.get_field_qtypes(P.args),
                      P.arg0,
                  )
              ),
              'disabled',
          ),
      ],
      qtype_inference_expr=M_seq.reduce(
          M_qtype.common_qtype, M_qtype.get_field_qtypes(P.args), P.arg0
      ),
  )
  def _join_with_separator(sep, arg0, *args):
    raise NotImplementedError('provided by backend')

  return M_core.apply_varargs(
      _lift_dynamically(_join_with_separator), sep, arg0, args
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'strings.join',
    qtype_constraints=[
        constraints.expect_texts_or_byteses(P.arg0),
        (
            (
                M_seq.reduce(
                    M_qtype.common_qtype,
                    M_qtype.get_field_qtypes(P.args),
                    P.arg0,
                )
                != arolla.NOTHING
            ),
            (
                'expected all arguments to have compatible string types, got'
                f' {constraints.name_type_msg(P.arg0)},'
                f' {constraints.variadic_name_type_msg(P.args)}'
            ),
        ),
    ],
)
def join(arg0, *args):
  """Returns the result of the concatenation of the strings."""
  args = arolla.optools.fix_trace_args(args)
  empty_str_op = arolla.types.DispatchOperator(
      'arg0',
      text_case=arolla.types.DispatchCase(
          arolla.text(''),
          condition=(M_qtype.get_scalar_qtype(P.arg0) == arolla.TEXT),
      ),
      default=arolla.bytes(b''),
  )
  return M_core.apply_varargs(
      join_with_separator, empty_str_op(arg0), arg0, args
  )
