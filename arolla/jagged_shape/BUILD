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
    name = "jagged_shape",
    srcs = [],
    hdrs = [
        "jagged_shape.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "jagged_shape_test",
    timeout = "long",
    srcs = ["jagged_shape_test.cc"],
    deps = [
        ":jagged_shape",
        "//arolla/array",
        "//arolla/dense_array",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/dense_array",
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "benchmarks_smoke_test",
    binary = ":jagged_shape_test",
)
