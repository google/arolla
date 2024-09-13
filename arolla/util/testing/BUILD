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
    testonly = True,
    hdrs = [
        "equals_proto.h",
        "repr_token_eq.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = [
        "keep_dep",  # Keep test_env linked to the test binaries.
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":test_env",
        "//arolla/util",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "test_env",
    testonly = True,
    srcs = ["test_env.cc"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/util",
        "@com_google_googletest//:gtest",
    ],
    alwayslink = True,
)

cc_test(
    name = "equals_proto_test",
    srcs = ["equals_proto_test.cc"],
    deps = [
        ":test_cc_proto",
        ":testing",
        "@com_google_googletest//:gtest_main",
    ],
)

proto_library(
    name = "test_proto",
    testonly = True,
    srcs = ["test.proto"],
)

cc_proto_library(
    name = "test_cc_proto",
    testonly = True,
    deps = [":test_proto"],
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
