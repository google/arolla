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

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "qexpr",
    srcs = [
        "bound_operators.cc",
        "casting.cc",
        "casting.h",
        "evaluation_engine.cc",
        "generated_operator.cc",
        "operator_errors.cc",
        "operator_factory.cc",
        "operator_metadata.cc",
        "operators.cc",
        "qexpr_operator_signature.cc",
        "simple_executable.cc",
    ],
    hdrs = [
        "aggregation_ops_interface.h",
        "bound_operators.h",
        "eval_context.h",
        "evaluation_engine.h",
        "generated_operator.h",
        "lift_accumulator_to_scalar_operator.h",
        "lift_to_optional_operator.h",
        "lifting.h",
        "operator_errors.h",
        "operator_factory.h",
        "operator_metadata.h",
        "operators.h",
        "qexpr_operator_signature.h",
        "result_type_traits.h",
        "simple_executable.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "bound_operators_test",
    srcs = [
        "bound_operators_test.cc",
    ],
    deps = [
        ":qexpr",
        "//arolla/memory",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "operator_errors_test",
    srcs = [
        "operator_errors_test.cc",
    ],
    deps = [
        ":qexpr",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "eval_context_test",
    srcs = ["eval_context_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "operators_test",
    srcs = ["operators_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/codegen/qexpr/testing:test_operators_lib",
        "//arolla/memory",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "lifting_test",
    srcs = [
        "lifting_test.cc",
    ],
    deps = [
        ":qexpr",
        "//arolla/memory",
        "//arolla/qexpr/testing",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "operators_benchmark",
    testonly = 1,
    srcs = ["operators_benchmark.cc"],
    deps = [
        ":qexpr",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "operators_benchmark_smoke_test",
    benchmarks = "BM_AddFloat/32/",
    binary = "operators_benchmark",
)

cc_test(
    name = "operator_factory_test",
    srcs = ["operator_factory_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "casting_test",
    srcs = [
        "casting.h",
        "casting_test.cc",
    ],
    deps = [
        ":qexpr",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "operator_metadata_test",
    srcs = ["operator_metadata_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "lift_accumulator_to_scalar_operator_test",
    srcs = ["lift_accumulator_to_scalar_operator_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/memory",
        "//arolla/qexpr/operators/testing",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "lift_to_optional_operator_test",
    srcs = ["lift_to_optional_operator_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "simple_executable_test",
    srcs = ["simple_executable_test.cc"],
    deps = [
        ":qexpr",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/memory",
        "//arolla/qexpr/testing",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_googletest//:gtest_main",
    ],
)
