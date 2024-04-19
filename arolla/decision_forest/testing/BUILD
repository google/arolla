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

package(default_visibility = ["//arolla/decision_forest:__subpackages__"])

licenses(["notice"])

cc_library(
    name = "testing",
    testonly = 1,
    srcs = [
        "test_util.cc",
    ],
    hdrs = [
        "test_util.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/decision_forest",
        "//arolla/decision_forest/split_conditions",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "test_util_test",
    srcs = ["test_util_test.cc"],
    deps = [
        ":testing",
        "//arolla/decision_forest",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_googletest//:gtest_main",
    ],
)
