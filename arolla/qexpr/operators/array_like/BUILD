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

# Array and DenseArray ops with shared (templated) implementations. Specific implementations should
# be kept in the respective packages.

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_library(
    name = "array_like",
    srcs = [],
    hdrs = ["edge_ops.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)
