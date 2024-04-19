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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

# All the QExpr-level operators provided by the Arolla Team.
cc_library(
    name = "all",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/qexpr/eval_extensions",
        "//arolla/qexpr/operators/aggregation",
        "//arolla/qexpr/operators/array",
        "//arolla/qexpr/operators/bitwise",
        "//arolla/qexpr/operators/bool",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qexpr/operators/core",
        "//arolla/qexpr/operators/dense_array",
        "//arolla/qexpr/operators/dict",
        "//arolla/qexpr/operators/experimental/dense_array",
        "//arolla/qexpr/operators/math",
        "//arolla/qexpr/operators/math_extra",
        "//arolla/qexpr/operators/random",
        "//arolla/qexpr/operators/seq",
        "//arolla/qexpr/operators/strings",
    ],
)

# Metadatas for all the QExpr-level operators.
cc_library(
    name = "metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        "//arolla/qexpr/operators/aggregation:operators_metadata",
        "//arolla/qexpr/operators/bitwise:operators_metadata",
        "//arolla/qexpr/operators/bool:operators_metadata",
        "//arolla/qexpr/operators/bootstrap:operators_metadata",
        "//arolla/qexpr/operators/core:operators_metadata",
        "//arolla/qexpr/operators/dense_array:operators_metadata",
        "//arolla/qexpr/operators/dict:operators_metadata",
        "//arolla/qexpr/operators/experimental/dense_array:operators_metadata",
        "//arolla/qexpr/operators/math:operators_metadata",
        "//arolla/qexpr/operators/math_extra:operators_metadata",
        "//arolla/qexpr/operators/random:operators_metadata",
        "//arolla/qexpr/operators/strings:operators_metadata",
    ],
)

# Set of operators sufficient to evaluate models on scalars.
cc_library(
    name = "operators_on_scalars",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/qexpr/eval_extensions",
        "//arolla/qexpr/operators/bitwise:operators_on_scalars",
        "//arolla/qexpr/operators/bool:operators_on_scalars",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qexpr/operators/core:operators_on_scalars",
        "//arolla/qexpr/operators/dense_array:operator_at_on_scalars",  # for dict operators
        "//arolla/qexpr/operators/dense_array:operator_make_dense_array",  # for dict & curve operators
        "//arolla/qexpr/operators/dense_array:operator_unique",  # for MatchingValues
        "//arolla/qexpr/operators/dict:operators_on_scalars",
        "//arolla/qexpr/operators/math:operators_on_scalars",
        "//arolla/qexpr/operators/random:operators_on_scalars",
        "//arolla/qexpr/operators/strings:operators_on_scalars",
    ],
)

# Set of operators sufficient to evaluate models on batches using DenseArray.
cc_library(
    name = "operators_on_dense_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        ":operators_on_scalars",
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/qexpr/eval_extensions",
        "//arolla/qexpr/operators/aggregation:operators_on_dense_arrays",
        "//arolla/qexpr/operators/bitwise:operators_on_dense_arrays",
        "//arolla/qexpr/operators/bool:operators_on_dense_arrays",
        "//arolla/qexpr/operators/core:operators_on_dense_arrays",
        "//arolla/qexpr/operators/dense_array",
        "//arolla/qexpr/operators/dict:operators_on_dense_arrays",
        "//arolla/qexpr/operators/math:operators_on_dense_arrays",
        "//arolla/qexpr/operators/strings:operators_on_dense_arrays",
    ],
)

# Set of operators sufficient to evaluate models on batches using Array.
cc_library(
    name = "operators_on_arrays",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        ":operators_on_scalars",
        "//arolla/decision_forest/qexpr_operator",
        "//arolla/qexpr/eval_extensions",
        "//arolla/qexpr/operators/aggregation:operators_on_arrays",
        "//arolla/qexpr/operators/array",
        "//arolla/qexpr/operators/bitwise:operators_on_arrays",
        "//arolla/qexpr/operators/bool:operators_on_arrays",
        "//arolla/qexpr/operators/core:operators_on_arrays",
        "//arolla/qexpr/operators/dict:operators_on_arrays",
        "//arolla/qexpr/operators/math:operators_on_arrays",
        "//arolla/qexpr/operators/strings:operators_on_arrays",
    ],
)
