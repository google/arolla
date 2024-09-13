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

# Examples of C++ API.

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

cc_test(
    name = "agg_execution_test",
    srcs = ["agg_execution_test.cc"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators/all",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
