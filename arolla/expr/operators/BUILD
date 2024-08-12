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

# The minimal set of operators demanded by the Expr -> QExpr Compiler.
cc_library(
    name = "bootstrap",
    srcs = [
        "bootstrap_operators.cc",
        "casting_registry.cc",
        "derived_qtype_operators.cc",
        "map_operator.cc",
        "meta_operators.cc",
        "std_function_operator.cc",
        "tuple_bootstrap_operators.cc",
        "type_meta_eval_strategies.cc",
        "weak_qtype_operators.cc",
    ],
    hdrs = [
        "bootstrap_operators.h",
        "casting_registry.h",
        "derived_qtype_operators.h",
        "map_operator.h",
        "meta_operators.h",
        "registration.h",
        "restricted_operator.h",
        "std_function_operator.h",
        "tuple_bootstrap_operators.h",
        "type_meta_eval_strategies.h",
        "weak_qtype_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/standard_type_properties",
        "//arolla/qtype/strings",
        "//arolla/sequence",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

cc_library(
    name = "operators",
    srcs = [
        "aggregation.cc",
        "dynamic_lifting.cc",
        "factory_operators.cc",
        "register_operators.cc",
        "restricted_operator.cc",
    ],
    hdrs = [
        "aggregation.h",
        "dynamic_lifting.h",
        "factory_operators.h",
        "register_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":bootstrap",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "type_meta_eval_strategies_test",
    srcs = ["type_meta_eval_strategies_test.cc"],
    deps = [
        ":bootstrap",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "casting_registry_test",
    srcs = ["casting_registry_test.cc"],
    deps = [
        ":bootstrap",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "factory_operators_test",
    srcs = ["factory_operators_test.cc"],
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "core_operators_test",
    srcs = ["core_operators_test.cc"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "registration_test",
    srcs = ["registration_test.cc"],
    deps = [
        ":bootstrap",
        "//arolla/expr",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "restricted_operator_test",
    srcs = ["restricted_operator_test.cc"],
    deps = [
        ":bootstrap",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "dynamic_lifting_test",
    srcs = ["dynamic_lifting_test.cc"],
    deps = [
        ":bootstrap",
        ":operators",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "weak_qtype_operators_test",
    srcs = ["weak_qtype_operators_test.cc"],
    deps = [
        ":bootstrap",
        "//arolla/array",
        "//arolla/array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "std_function_operator_test",
    srcs = ["std_function_operator_test.cc"],
    deps = [
        ":bootstrap",
        "//arolla/array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)
