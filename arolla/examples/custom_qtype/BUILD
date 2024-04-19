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

# Example of custom QType definition.
#
# See py/arolla/examples/custom_qtype/ for the Python part.

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "complex",
    srcs = ["complex.cc"],
    hdrs = ["complex.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "complex_test",
    srcs = ["complex_test.cc"],
    deps = [
        ":complex",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "complex_operators",
    srcs = ["complex_operators.cc"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":complex",
        "//arolla/optools",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
    ],
    alwayslink = 1,
)
