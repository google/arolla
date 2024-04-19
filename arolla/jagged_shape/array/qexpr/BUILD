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
    "operator_libraries",
    "operator_overload",
)

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

operator_lib_list = [
    # go/keep-sorted start
    ":operator_jagged_array_shape_add_dims",
    ":operator_jagged_array_shape_edge_at",
    ":operator_jagged_array_shape_edges",
    ":operator_jagged_array_shape_flatten",
    ":operator_jagged_array_shape_from_edges",
    ":operator_jagged_array_shape_rank",
    ":operator_jagged_array_shape_remove_dims",
    ":operator_jagged_array_shape_size",
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
    hdrs = ["shape_operators.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/array/qtype",
        "//arolla/jagged_shape/array",
        "//arolla/jagged_shape/array/qtype",
        "//arolla/jagged_shape/qexpr:lib",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
    ],
)

operator_family(
    name = "operator_jagged_array_shape_from_edges",
    hdrs = ["shape_operators.h"],
    op_family_class = "::arolla::JaggedArrayShapeFromEdgesOperatorFamily",
    op_family_name = "jagged.array_shape_from_edges",
    deps = [":lib"],
)

operator_family(
    name = "operator_jagged_array_shape_add_dims",
    hdrs = ["shape_operators.h"],
    op_family_class = "::arolla::JaggedArrayShapeAddDimsOperatorFamily",
    op_family_name = "jagged.add_dims._array",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_jagged_array_shape_rank",
    operator_name = "jagged.rank",
    overloads = [operator_overload(
        hdrs = ["shape_operators.h"],
        args = ("::arolla::JaggedArrayShapePtr",),
        op_class = "::arolla::JaggedArrayShapeRankOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_array_shape_edge_at",
    operator_name = "jagged.edge_at",
    overloads = [operator_overload(
        hdrs = ["shape_operators.h"],
        args = ("::arolla::JaggedArrayShapePtr", "int64_t"),
        op_class = "::arolla::JaggedArrayShapeEdgeAtOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_array_shape_remove_dims",
    operator_name = "jagged.remove_dims",
    overloads = [operator_overload(
        hdrs = ["shape_operators.h"],
        args = ("::arolla::JaggedArrayShapePtr", "int64_t"),
        op_class = "::arolla::JaggedArrayShapeRemoveDimsOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_jagged_array_shape_flatten",
    operator_name = "jagged._flatten",
    overloads = [operator_overload(
        hdrs = ["shape_operators.h"],
        args = ("::arolla::JaggedArrayShapePtr", "int64_t", "int64_t"),
        op_class = "::arolla::JaggedArrayShapeFlattenOp",
        deps = [":lib"],
    )],
)

operator_family(
    name = "operator_jagged_array_shape_edges",
    hdrs = ["shape_operators.h"],
    op_family_class = "::arolla::JaggedArrayShapeEdgesOperatorFamily",
    op_family_name = "jagged.edges._array",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_jagged_array_shape_size",
    operator_name = "jagged.size",
    overloads = [operator_overload(
        hdrs = ["shape_operators.h"],
        args = ("::arolla::JaggedArrayShapePtr",),
        op_class = "::arolla::JaggedArrayShapeSizeOp",
        deps = [":lib"],
    )],
)
