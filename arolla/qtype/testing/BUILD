# Copyright 2025 Google LLC
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

# QType-related tools for testing

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    hdrs = ["matchers.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:nullability",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "matchers_test",
    srcs = ["matchers_test.cc"],
    deps = [
        ":testing",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "dummy_types",
    testonly = 1,
    srcs = [
        "dummy_types.cc",
    ],
    hdrs = [
        "dummy_types.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
    ],
)
