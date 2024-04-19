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

# TODO: describe this package.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "default",
    srcs = ["default_optimizer.cc"],
    hdrs = ["default_optimizer.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr/optimization",
        "//arolla/expr/optimization/peephole_optimizations",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status:statusor",
    ],
)
