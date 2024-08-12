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
    name = "expr",
    srcs = [
        "annotation_expr_operators.cc",
        "annotation_utils.cc",
        "backend_wrapping_operator.cc",
        "basic_expr_operator.cc",
        "derived_qtype_cast_operator.cc",
        "expr.cc",
        "expr_attributes.cc",
        "expr_debug_string.cc",
        "expr_node.cc",
        "expr_node_ptr.h",
        "expr_operator.cc",
        "expr_operator_signature.cc",
        "expr_stack_trace.cc",
        "expr_visitor.cc",
        "lambda_expr_operator.cc",
        "operator_repr_functions.cc",
        "overloaded_expr_operator.cc",
        "qtype_utils.cc",
        "quote.cc",
        "registered_expr_operator.cc",
        "seq_map_expr_operator.cc",
        "seq_reduce_expr_operator.cc",
        "tuple_expr_operator.cc",
    ],
    hdrs = [
        "annotation_expr_operators.h",
        "annotation_utils.h",
        "backend_wrapping_operator.h",
        "basic_expr_operator.h",
        "derived_qtype_cast_operator.h",
        "expr.h",
        "expr_attributes.h",
        "expr_debug_string.h",
        "expr_node.h",
        "expr_operator.h",
        "expr_operator_signature.h",
        "expr_stack_trace.h",
        "expr_visitor.h",
        "lambda_expr_operator.h",
        "operator_repr_functions.h",
        "overloaded_expr_operator.h",
        "qtype_utils.h",
        "quote.h",
        "registered_expr_operator.h",
        "seq_map_expr_operator.h",
        "seq_reduce_expr_operator.h",
        "tuple_expr_operator.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/sequence",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/numeric:int128",
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
    name = "annotation_expr_operators_test",
    srcs = ["annotation_expr_operators_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "annotation_utils_test",
    srcs = ["annotation_utils_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_debug_string_test",
    srcs = ["expr_debug_string_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/qtype/testing:dummy_types",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_node_test",
    srcs = ["expr_node_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "expr_benchmarks",
    testonly = 1,
    srcs = [
        "expr_benchmarks.cc",
    ],
    deps = [
        ":expr",
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "expr_benchmarks_smoke_test",
    binary = ":expr_benchmarks",
)

cc_test(
    name = "expr_attributes_test",
    srcs = ["expr_attributes_test.cc"],
    deps = [
        ":expr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_visitor_test",
    srcs = ["expr_visitor_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "quote_test",
    srcs = ["quote_test.cc"],
    deps = [
        ":expr",
        "//arolla/dense_array",
        "//arolla/expr/testing:test_operators",
        "//arolla/util",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_operator_signature_test",
    srcs = ["expr_operator_signature_test.cc"],
    deps = [
        ":expr",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_operator_test",
    srcs = ["expr_operator_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_stack_trace_test",
    srcs = ["expr_stack_trace_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "lambda_expr_operator_test",
    srcs = ["lambda_expr_operator_test.cc"],
    deps = [
        ":expr",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "overloaded_expr_operator_test",
    srcs = ["overloaded_expr_operator_test.cc"],
    deps = [
        ":expr",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "derived_qtype_cast_operator_test",
    srcs = ["derived_qtype_cast_operator_test.cc"],
    deps = [
        ":expr",
        "//arolla/codegen/qexpr/testing:test_operators",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_test",
    srcs = ["expr_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "qtype_utils_test",
    srcs = ["qtype_utils_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "tuple_expr_operator_test",
    srcs = ["tuple_expr_operator_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "registered_expr_operator_test",
    srcs = ["registered_expr_operator_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "operator_repr_functions_test",
    srcs = ["operator_repr_functions_test.cc"],
    deps = [
        ":expr",
        "//arolla/expr/testing:test_operators",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
