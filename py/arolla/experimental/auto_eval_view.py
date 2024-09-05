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

"""AutoEvalView is an interactive feature to show eval result beside Expr.

In IPython environment, the eval result of an expression is shown together with
the expression text when the expression does not contain any free variables.

The feature is enabled by importing this module.
"""

from concurrent import futures
from typing import Any

from arolla import arolla

# How long we block on evaluating an expression while printing.
EVAL_TIMEOUT_SEC = 2.0

# A marker to indicate that eval result is not available.
_AUTO_EVAL_FAILED = object()

_AUTO_EVAL_EXECUTOR = futures.ThreadPoolExecutor(1)


def _display_eval_result(eval_result: Any):
  """Displays the eval result."""
  from IPython import display  # pylint: disable=g-import-not-at-top  # pytype: disable=import-error

  display.display(display.HTML('<hr><b>eval:</b> '), eval_result)


def _try_auto_eval(expr: arolla.Expr) -> Any:
  """A helper for the asynchronous evaluation of `expr`."""
  # Skip if the expression contains any free variables.
  if arolla.abc.get_leaf_keys(expr) or arolla.abc.get_placeholder_keys(expr):
    return _AUTO_EVAL_FAILED

  task = _AUTO_EVAL_EXECUTOR.submit(arolla.eval, expr)
  try:
    return task.result(EVAL_TIMEOUT_SEC)
  except futures.TimeoutError:
    return _AUTO_EVAL_FAILED
  except ValueError as ex:
    # When the eval raised an exception, print the exception as the result.
    return ex


def _auto_eval_view_ipython_display_(expr: arolla.Expr):
  """Overrides the display format in IPython environment."""
  # Normal text rendering of the expr.
  print(expr)

  # In addition, print the eval result if available.
  eval_result = _try_auto_eval(expr)
  if eval_result is not _AUTO_EVAL_FAILED:
    _display_eval_result(eval_result)


def enable():
  """Enables AutoEvalView."""
  arolla.abc.unsafe_register_default_expr_view_member(
      '_ipython_display_', _auto_eval_view_ipython_display_
  )


def disable():
  """Disables AutoEvalView."""
  arolla.abc.unsafe_remove_default_expr_view_member('_ipython_display_')


# Importing this module enables it.
enable()
