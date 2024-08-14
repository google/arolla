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

# Internal test operator definitions

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "accumulator_overload",
    "lift_accumulator_to_scalar",
    "lift_by",
    "operator_libraries",
)

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":operator_agg_sum",
        ":operator_average",
        ":operator_rank_values",
    ],
    alwayslink = 1,
)

cc_library(
    name = "lib",
    testonly = 1,
    hdrs = [
        "accumulators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
    ],
)

operator_libraries(
    name = "operator_agg_sum",
    testonly = 1,
    operator_name = "test.agg_sum",
    overloads = lift_by(
        [lift_accumulator_to_scalar],
        [
            accumulator_overload(
                hdrs = ["accumulators.h"],
                acc_class = "::arolla::testing::AggSumAccumulator<int>",
                child_args = ["int"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_average",
    testonly = 1,
    operator_name = "test.average",
    overloads = lift_by(
        [lift_accumulator_to_scalar],
        [
            accumulator_overload(
                hdrs = ["accumulators.h"],
                acc_class = "::arolla::testing::AverageAccumulator",
                child_args = ["float"],
                deps = [":lib"],
            ),
        ],
    ),
)

operator_libraries(
    name = "operator_rank_values",
    testonly = 1,
    operator_name = "test.rank_values",
    overloads = lift_by(
        [lift_accumulator_to_scalar],
        [
            accumulator_overload(
                hdrs = ["accumulators.h"],
                acc_class = "::arolla::testing::RankValuesAccumulator<int>",
                child_args = ["int"],
                deps = [":lib"],
            ),
        ],
    ),
)
