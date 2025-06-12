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

"""Operator declaration helpers."""

import inspect
import types
from typing import Any, Callable

from arolla.abc import abc as arolla_abc
from arolla.types import types as arolla_types


def make_lambda(
    *args: Any,
    qtype_constraints: arolla_types.QTypeConstraints = (),
    name: str = 'anonymous.lambda',
    doc: str = '',
) -> arolla_abc.Operator:
  """Returns a lambda operator.

  This function creates a lambda operator based on the provided signature and
  lambda body expression. Depending on whether `qtype_constraints` are provided,
  it returns either a lambda operator or a restricted lambda operator.

  Args:
    *args: The signature and/or the lambda body expression.
    qtype_constraints: A list of `[predicate_expr, error_message]` pairs that
      define constraints on the types of the arguments. If a constraint is not
      fulfilled, the corresponding error_message is used. Placeholders, such as
      `{arg_name}`, are replaced with the actual type names during error message
      formatting.
    name: The name of the operator. Defaults to 'anonymous.lambda'.
    doc: The docstring for the operator.

  Returns:
    A lambda operator (or a restricted lambda operator, depending on
    the presence of `qtype_constraints`).
  """
  if not qtype_constraints:
    return arolla_types.LambdaOperator(*args, name=name, doc=doc)
  return arolla_types.RestrictedLambdaOperator(
      *args, qtype_constraints=qtype_constraints, name=name, doc=doc
  )


# TOOD: b/383536303 - Consider improving the error messages for "unfixed"
# variadic `*args` and `**kwargs` during tracing.
def trace_function(
    fn: types.FunctionType,
    *,
    gen_tracer: Callable[[str], arolla_abc.Expr] = arolla_abc.placeholder,
):
  """Traces a function and returns an expression representing its computation.

  This function executes the given function `fn`, with the "tracers" arguments,
  and returns the result.

  It's recommended to use fix_trace_args_kwargs for the variadic parameters:

    ```python
    @arolla.optools.trace_function
    def fn(x, *y, **z):
      y, z = arolla.optools.fix_trace_args_kwargs(y, z)
      return [x, y, z]

    print(fn)  # [P.x, P.y, P.z]
    ```

  Args:
    fn: The function to trace. Must be a `function` object.
    gen_tracer: A callable that returns a tracing expression for a function
      parameter. If not provided, it defaults to `arolla.abc.placeholder`.

  Returns:
    The result of executing the function `fn` with the tracer arguments.
  """
  if not isinstance(fn, types.FunctionType):
    raise TypeError(
        'expected a `function` object, got'
        f' {arolla_abc.get_type_name(type(fn))}'
    )
  tracing_args = {}
  tracing_kwargs = {}
  for param in inspect.signature(fn).parameters.values():
    if (
        param.kind == param.POSITIONAL_ONLY
        or param.kind == param.POSITIONAL_OR_KEYWORD
        or param.kind == param.VAR_POSITIONAL
    ):
      tracing_args[param.name] = gen_tracer(param.name)
    elif param.kind == param.KEYWORD_ONLY or param.kind == param.VAR_KEYWORD:
      tracing_kwargs[param.name] = gen_tracer(param.name)
    else:
      raise TypeError(f'unexpected parameter: {param}')
  return fn(*tracing_args.values(), **tracing_kwargs)


def fix_trace_args(args: tuple[arolla_abc.Expr, ...], /) -> arolla_abc.Expr:
  """Fixes the variadic-positional argument during a `trace_function(...)` call.

  When the `*args` argument is traced using `@arolla.optools.trace_function`,
  it is passed as a regular tuple. To transform it into a tracer object
  (`arolla.Expr`), you need to apply `args = fix_trace_args(args)`.

  Example of usage:
    ```python
    @arolla.optools.trace_function
    def fn(x, *y):
      y = arolla.optools.fix_trace_args(y)
      return [x, y]

    print(fn)  # [P.x, P.y]
    ```

  Args:
    args: The variadic-positional argument from the traced function.

  Returns:
    A single `arolla.Expr` object representing the variadic argument.
  """
  if (
      isinstance(args, tuple)
      and len(args) == 1
      and isinstance(args[0], arolla_abc.Expr)
  ):
    return args[0]
  raise TypeError('expected `*args` provided by `trace_function(...)`')


def fix_trace_kwargs(kwargs: dict[str, arolla_abc.Expr], /) -> arolla_abc.Expr:
  """Fixes the variadic-keyword argument during a `trace_function(...)` call.

  When the `**kwargs` argument is traced using `@arolla.optools.trace_function`,
  it is passed as a regular dictionary. To transform it into a tracer object
  (`arolla.Expr`), you need to apply `kwargs = fix_trace_kwargs(kwargs)`.

  Example of usage:
    ```python
    @arolla.optools.trace_function
    def fn(x, **y):
      y = arolla.optools.fix_trace_kwargs(y)
      return [x, y]

    print(fn)  # [P.x, P.y]
    ```

  Args:
    kwargs: The variadic-keyword argument from the traced function.

  Returns:
    A single `arolla.Expr` object representing the variadic argument.
  """
  if isinstance(kwargs, dict) and len(kwargs) == 1:
    for k, v in kwargs.items():
      if isinstance(k, str) and isinstance(v, arolla_abc.Expr):
        return v
  raise TypeError('expected `**kwargs` provided by `trace_function(...)`')


def fix_trace_args_kwargs(
    args: tuple[arolla_abc.Expr, ...], kwargs: dict[str, arolla_abc.Expr], /
) -> tuple[arolla_abc.Expr, arolla_abc.Expr]:
  """Alias for `(fix_trace_args(args), fix_trace_kwargs(kwargs))`."""
  return fix_trace_args(args), fix_trace_kwargs(kwargs)
