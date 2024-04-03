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

"""Basic eval facilities."""

from arolla.abc import clib
from arolla.abc import utils as abc_utils

# A compiled expression ready for execution.
#
# IMPORTANT: The primary purpose of this class is to be a low-level building
# block. Particularly, it doesn't implement any caching facility. You should
# possibly prefer using rl.abc.eval_expr().
#
# class CompileExpr:
#
#   Methods defined here:
#
#     __new__(
#         cls,
#         expr: Expr,
#         input_qtypes: dict[str, QType],
#         *,
#         options: dict[str, Any] = {}
#     ):
#       Compiles the given expression.
#
#     __call__(self, *args: QValue, **kwargs: QValue) -> AnyQValue:
#       Returns the evaluation result.
#
#     execute(self, input_qvalues: dict[str, QValue]) -> AnyQValue:
#       Returns the evaluation result.
#
# (implementation: py/arolla/abc/py_compiled_expr.cc)
#
CompiledExpr = clib.CompiledExpr

# Invokes the operator with the given inputs and returns the result.
invoke_op = clib.invoke_op

# Compiles and executes an expression for the given inputs.
eval_expr = clib.eval_expr

# Returns the result of an operator evaluation with given arguments.
aux_eval_op = clib.aux_eval_op

abc_utils.cache_clear_callbacks.add(
    clib.clear_eval_compile_cache
)  # subscribe the lru_cache for cleaning
