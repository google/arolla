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
    name = "split_conditions",
    hdrs = [
        "interval_split_condition.h",
        "set_of_values_split_condition.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/decision_forest",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "split_conditions_test",
    srcs = ["split_conditions_test.cc"],
    deps = [
        ":split_conditions",
        "//arolla/decision_forest",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
