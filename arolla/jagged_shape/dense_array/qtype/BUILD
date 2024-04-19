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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "qtype",
    srcs = ["qtype.cc"],
    hdrs = ["qtype.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/jagged_shape/dense_array",
        "//arolla/jagged_shape/qtype",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = ["qtype_test.cc"],
    deps = [
        ":qtype",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/array/qtype",
        "//arolla/jagged_shape/dense_array",
        "//arolla/jagged_shape/qtype",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)
