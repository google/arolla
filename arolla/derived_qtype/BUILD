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

# Package with additional tooling for Derived QTypes
#
# The fundamental implementation of derived QTypes is currently part of the core library, primarily
# because:
# - it's used for the implementation of weak_float.
# - it's used for the implementation of namedtuples.
# - the derived upcasting/downcasting has custom compiler support to achieve zero-copy.
#
# However, it's preferable to add any new derived QType implementations and features here rather
# than in the core library, to limit its complexity. Additionally, if it's possible to refactor some
# core code and move more logic, it might also be preferable to move it here. Importantly, the core
# libraries shouldn't depend on this package (otherwise, it becomes a part of the core itself).

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

licenses(["notice"])

cc_library(
    name = "derived_qtype",
    srcs = ["labeled_qtype.cc"],
    hdrs = ["labeled_qtype.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "labeled_qtype_test",
    srcs = ["labeled_qtype_test.cc"],
    deps = [
        ":derived_qtype",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
