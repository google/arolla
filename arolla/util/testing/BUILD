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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_python//python:defs.bzl", "py_binary")

package(
    default_testonly = 1,
    default_visibility = ["//visibility:private"],
)

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "status_matchers_backport.cc",
    ],
    hdrs = [
        "equals_proto.h",
        "repr_token_eq.h",
        "status_matchers_backport.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = [
        "//arolla:internal",
        "//py/arolla/expr/codegen/testing:__subpackages__",
    ],
    deps = [
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
)

py_binary(
    name = "run_benchmark_smoke_test",
    srcs = ["run_benchmark_smoke_test.py"],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl_py//absl:app",
        "@com_google_absl_py//absl/flags",
    ],
)

exports_files(["run_smoke_test.sh"])

bzl_library(
    name = "testing_bzl",
    srcs = ["testing.bzl"],
    visibility = ["//visibility:public"],
    deps = ["//arolla/codegen:utils_bzl"],
)
