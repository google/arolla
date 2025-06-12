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

# Tools for codegen tests that are extracted in the separate directory to allow usage of internals.

load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//py/arolla/codegen/testing:__subpackages__"])

licenses(["notice"])

# library registering "always_0" optimization with
# ::arolla::codegen::RegisterOptimization.
cc_library(
    name = "always_0_optimization",
    testonly = 1,
    srcs = ["always_0_optimization.cc"],
    deps = [
        "//arolla/codegen/expr",
        "//arolla/expr",
        "//arolla/expr/optimization",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
    ],
    alwayslink = 1,
)

cc_library(
    name = "override_type_repr",
    testonly = 1,
    srcs = ["override_type_repr.cc"],
    deps = [
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
    alwayslink = 1,
)
