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

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "operator_family",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    ":operator_seq_range",
]

# Registers all operators defined in the package.
cc_library(
    name = "seq",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_lib_list,
)

# Registers metadata for all the operators defined in the package.
cc_library(
    name = "operators_metadata",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [lib + "_metadata" for lib in operator_lib_list],
)

# Implementation for operators defined in the package.
cc_library(
    name = "lib",
    hdrs = [
        "sequence_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

operator_family(
    name = "operator_seq_range",
    hdrs = ["sequence_operators.h"],
    op_family_class = "::arolla::SequenceRangeOpFamily",
    op_family_name = "seq.range",
    deps = [":lib"],
)
