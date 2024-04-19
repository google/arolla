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

# Infrastructure to perform optimizations on expressions.

package(default_visibility = [
    "//arolla:internal",
])

licenses(["notice"])

cc_library(
    name = "optimization",
    srcs = [
        "optimizer.cc",
        "peephole_optimizer.cc",
    ],
    hdrs = [
        "optimizer.h",
        "peephole_optimizer.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = [
        "//arolla:internal",
        "//video/content_analysis/match/siberia/match_analysis:__subpackages__",
    ],
    deps = [
        "//arolla/expr",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "peephole_optimizer_test",
    srcs = ["peephole_optimizer_test.cc"],
    deps = [
        ":optimization",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/expr/visitors",
        "//arolla/memory",
        "//arolla/qexpr/operators/all",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "optimizer_test",
    srcs = ["optimizer_test.cc"],
    deps = [
        ":optimization",
        "//arolla/expr",
        "//arolla/expr/operators/all",
        "//arolla/expr/testing",
        "//arolla/qexpr/operators/all",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)
