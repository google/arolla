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

# Test utilities for jagged shape.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = True,
    hdrs = ["matchers.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/jagged_shape",
        "//arolla/util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "matchers_test",
    srcs = ["matchers_test.cc"],
    deps = [
        ":testing",
        "//arolla/dense_array",
        "//arolla/jagged_shape/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)
