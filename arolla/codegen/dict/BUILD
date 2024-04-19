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

# literal and type extensions for py/arolla/expr/codegen

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "dict",
    srcs = ["dict_codegen_literal.cc"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
    ],
    alwayslink = 1,
)

cc_test(
    name = "dict_codegen_literal_test",
    srcs = ["dict_codegen_literal_test.cc"],
    deps = [
        ":dict",
        "//arolla/codegen/expr",
        "//arolla/qtype",
        "//arolla/qtype/dict",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
