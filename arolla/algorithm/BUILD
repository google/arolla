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

# Algorithms library

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "algorithm",
    srcs = [
        "control_flow_graph.cc",
    ],
    hdrs = [
        "control_flow_graph.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "control_flow_graph_test",
    srcs = ["control_flow_graph_test.cc"],
    deps = [
        ":algorithm",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)
