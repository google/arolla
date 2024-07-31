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
    "operator_libraries",
    "operator_overload",
    "qtype_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    # go/keep-sorted start
    ":operator_jagged_get_edge_qtype",
    ":operator_jagged_is_jagged_shape_qtype",
    ":operator_jagged_make_jagged_shape_qtype",
    # go/keep-sorted end
]

cc_library(
    name = "qexpr",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_lib_list,
)

cc_library(
    name = "lib",
    srcs = [],
    hdrs = [
        "qtype_operator.h",
        "shape_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/jagged_shape/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/sequence",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

operator_libraries(
    name = "operator_jagged_make_jagged_shape_qtype",
    operator_name = "jagged.make_jagged_shape_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type,),
        op_class = "::arolla::MakeJaggedShapeQTypeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_is_jagged_shape_qtype",
    operator_name = "jagged.is_jagged_shape_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type,),
        op_class = "::arolla::IsJaggedShapeQTypeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_get_edge_qtype",
    operator_name = "jagged.get_edge_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type,),
        op_class = "::arolla::GetEdgeQType",
        deps = [":lib"],
    )],
)
