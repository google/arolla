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

# Tools to create while loop operators.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "while_loop",
    srcs = [
        "while_loop.cc",
        "while_loop_impl.cc",
        "while_loop_impl.h",
    ],
    hdrs = [
        "while_loop.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "while_loop_test",
    srcs = ["while_loop_test.cc"],
    deps = [
        ":while_loop",
        "//arolla/expr",
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
    name = "while_loop_impl_test",
    srcs = [
        "while_loop_impl.h",
        "while_loop_impl_test.cc",
    ],
    deps = [
        ":while_loop",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
    ],
)
