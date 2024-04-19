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

# This library introduces a "lazy" type representing a value with
# deferred/on-demand computation.

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "lazy",
    srcs = [
        "lazy.cc",
        "lazy_qtype.cc",
    ],
    hdrs = [
        "lazy.h",
        "lazy_qtype.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "lazy_test",
    srcs = [
        "lazy_test.cc",
    ],
    deps = [
        ":lazy",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "lazy_qtype_test",
    srcs = [
        "lazy_qtype_test.cc",
    ],
    deps = [
        ":lazy",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "integration_test",
    srcs = [
        "integration_test.cc",
    ],
    deps = [
        ":lazy",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/lazy/operators",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
