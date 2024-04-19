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

# Tools for defining operators in C++.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "optools",
    srcs = ["optools.cc"],
    hdrs = ["optools.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "optools_test",
    srcs = ["optools_test.cc"],
    deps = [
        ":optools",
        "//arolla/expr",
        "//arolla/expr/testing",
        "//arolla/qtype",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)
