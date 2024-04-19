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

# Expr-level operators on strings

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "strings",
    srcs = [
        "register_operators.cc",
        "string_operators.cc",
    ],
    hdrs = [
        "register_operators.h",
        "string_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/operators",
        "//arolla/expr/operators:bootstrap",
        "//arolla/qtype",
        "//arolla/qtype/standard_type_properties",
        "//arolla/qtype/strings",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "string_operators_test",
    srcs = ["string_operators_test.cc"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)
