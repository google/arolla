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

# A minimal set of operators needed for arolla initialization:
#
#  * core.make_tuple
#  * core.equal(qtype, qtype)
#  * core.equal(int64, int64)
#  * core.not_equal(qtype, qtype)
#  * core.presence_and(optional_unit, optional_unit)
#  * core.presence_or(optional_unit, optional_unit)
#  * qtype.*
#
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "make_optional_type",
    "operator_family",
    "operator_libraries",
    "operator_overload",
    "operator_overload_list",
    "qtype_type",
    "unit_type",
)

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

operator_lib_list = [
    # go/keep-sorted start
    ":operator_core_equal",
    ":operator_core_make_tuple",
    ":operator_core_not_equal",
    ":operator_core_presence_and",
    ":operator_core_presence_not",
    ":operator_core_presence_or",
    ":operator_qtype__get_key_to_row_dict_qtype",
    ":operator_qtype_broadcast_qtype_like",
    ":operator_qtype_common_qtype",
    ":operator_qtype_conditional_qtype",
    ":operator_qtype_decay_derived_qtype",
    ":operator_qtype_get_child_shape_qtype",
    ":operator_qtype_get_edge_qtype",
    ":operator_qtype_get_edge_to_scalar_qtype",
    ":operator_qtype_get_field_count",
    ":operator_qtype_get_field_qtype",
    ":operator_qtype_get_field_qtypes",
    ":operator_qtype_get_parent_shape_qtype",
    ":operator_qtype_get_scalar_qtype",
    ":operator_qtype_get_shape_qtype",
    ":operator_qtype_get_value_qtype",
    ":operator_qtype_is_dict_qtype",
    ":operator_qtype_is_edge_qtype",
    ":operator_qtype_is_namedtuple_qtype",
    ":operator_qtype_is_sequence_qtype",
    ":operator_qtype_is_shape_qtype",
    ":operator_qtype_is_slice_qtype",
    ":operator_qtype_is_tuple_qtype",
    ":operator_qtype_make_dict_qtype",
    ":operator_qtype_make_sequence_qtype",
    ":operator_qtype_make_tuple_qtype",
    ":operator_qtype_slice_tuple_qtype",
    ":operator_qtype_with_value_qtype",
    ":operator_seq_at",
    ":operator_seq_size",
    ":operator_seq_slice",
    ":operator_seq_zip",
    # go/keep-sorted end
]

optional_unit_type = make_optional_type(unit_type)

# Registers all operators defined in the package.
cc_library(
    name = "bootstrap",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = operator_lib_list + [
        "//arolla/qexpr/eval_extensions",
    ],
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
    srcs = [
        "make_tuple_operator.cc",
        "sequence_operators.cc",
    ],
    hdrs = [
        "make_tuple_operator.h",
        "qtype_operator.h",
        "sequence_operators.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/qtype/dict",
        "//arolla/qtype/standard_type_properties",
        "//arolla/sequence",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

operator_libraries(
    name = "operator_core_equal",
    operator_name = "core.equal",
    overloads = operator_overload_list(
        hdrs = ["arolla/qexpr/operators/core/logic_operators.h"],
        arg_lists = [
            (qtype_type, qtype_type),
            ("int64_t", "int64_t"),
        ],
        op_class = "::arolla::MaskEqualOp",
        deps = ["//arolla/qexpr/operators/core:lib"],
    ),
)

operator_family(
    name = "operator_core_make_tuple",
    hdrs = ["make_tuple_operator.h"],
    op_family_class = "::arolla::MakeTupleOperatorFamily",
    op_family_name = "core.make_tuple",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_core_not_equal",
    operator_name = "core.not_equal",
    overloads = operator_overload_list(
        hdrs = ["arolla/qexpr/operators/core/logic_operators.h"],
        arg_lists = [(qtype_type, qtype_type)],
        op_class = "::arolla::MaskNotEqualOp",
        deps = ["//arolla/qexpr/operators/core:lib"],
    ),
)

operator_libraries(
    name = "operator_core_presence_and",
    operator_name = "core.presence_and",
    overloads = operator_overload_list(
        hdrs = ["arolla/qexpr/operators/core/logic_operators.h"],
        arg_as_function_ids = [0],
        arg_lists = [
            (optional_unit_type, optional_unit_type),
        ],
        op_class = "::arolla::PresenceAndOp",
        deps = ["//arolla/qexpr/operators/core:lib"],
    ),
)

operator_libraries(
    name = "operator_core_presence_not",
    operator_name = "core.presence_not._builtin",
    overloads = operator_overload_list(
        hdrs = ["arolla/qexpr/operators/core/logic_operators.h"],
        arg_lists = [(optional_unit_type,)],
        op_class = "::arolla::PresenceNotOp",
        deps = ["//arolla/qexpr/operators/core:lib"],
    ),
)

operator_libraries(
    name = "operator_core_presence_or",
    operator_name = "core.presence_or",
    overloads = operator_overload_list(
        hdrs = ["arolla/qexpr/operators/core/logic_operators.h"],
        arg_as_function_ids = [1],
        arg_lists = [
            (optional_unit_type, optional_unit_type),
        ],
        op_class = "::arolla::PresenceOrOp",
        deps = ["//arolla/qexpr/operators/core:lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_broadcast_qtype_like",
    operator_name = "qtype.broadcast_qtype_like",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type, qtype_type),
        ],
        op_class = "::arolla::BroadcastQTypeLikeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_common_qtype",
    operator_name = "qtype.common_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type, qtype_type),
        ],
        op_class = "::arolla::CommonQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_conditional_qtype",
    operator_name = "qtype.conditional_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (optional_unit_type, qtype_type, qtype_type),
        ],
        op_class = "::arolla::ConditionalQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_decay_derived_qtype",
    operator_name = "qtype.decay_derived_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::DecayDerivedQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_child_shape_qtype",
    operator_name = "qtype.get_child_shape_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetChildShapeQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_edge_qtype",
    operator_name = "qtype.get_edge_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetEdgeQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_edge_to_scalar_qtype",
    operator_name = "qtype.get_edge_to_scalar_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetEdgeToScalarQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_field_count",
    operator_name = "qtype.get_field_count",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetFieldCountOp",
        deps = [":lib"],
    ),
)

operator_family(
    name = "operator_qtype_get_field_qtypes",
    hdrs = ["qtype_operator.h"],
    op_family_class = "::arolla::GetFieldQTypesOpFamily",
    op_family_name = "qtype.get_field_qtypes",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_qtype_get_field_qtype",
    operator_name = "qtype.get_field_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type, "int64_t"),
        op_class = "::arolla::GetFieldQTypeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_qtype_get_parent_shape_qtype",
    operator_name = "qtype.get_parent_shape_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetParentShapeQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_scalar_qtype",
    operator_name = "qtype.get_scalar_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetScalarQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_shape_qtype",
    operator_name = "qtype.get_shape_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetShapeQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_get_value_qtype",
    operator_name = "qtype.get_value_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetValueQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype__get_key_to_row_dict_qtype",
    operator_name = "qtype._get_key_to_row_dict_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::GetKeyToRowDictQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_make_dict_qtype",
    operator_name = "qtype.make_dict_qtype",
    overloads = [operator_overload(
        hdrs = ["qtype_operator.h"],
        args = (qtype_type, qtype_type),
        op_class = "::arolla::MakeDictQTypeOp",
        deps = [":lib"],
    )],
)

operator_libraries(
    name = "operator_qtype_is_edge_qtype",
    operator_name = "qtype.is_edge_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsEdgeQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_is_sequence_qtype",
    operator_name = "qtype.is_sequence_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsSequenceQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_is_shape_qtype",
    operator_name = "qtype.is_shape_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsShapeQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_is_dict_qtype",
    operator_name = "qtype.is_dict_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsDictQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_is_tuple_qtype",
    operator_name = "qtype.is_tuple_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsTupleQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_is_namedtuple_qtype",
    operator_name = "qtype.is_namedtuple_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsNamedTupleQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_is_slice_qtype",
    operator_name = "qtype.is_slice_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::IsSliceQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_make_sequence_qtype",
    operator_name = "qtype.make_sequence_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type,),
        ],
        op_class = "::arolla::MakeSequenceQTypeOp",
        deps = [":lib"],
    ),
)

operator_family(
    name = "operator_qtype_make_tuple_qtype",
    hdrs = ["qtype_operator.h"],
    op_family_class = "::arolla::MakeTupleQTypeOpFamily",
    op_family_name = "qtype.make_tuple_qtype",
    deps = [":lib"],
)

operator_libraries(
    name = "operator_qtype_slice_tuple_qtype",
    operator_name = "qtype.slice_tuple_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type, "int64_t", "int64_t"),
        ],
        op_class = "::arolla::SliceTupleQTypeOp",
        deps = [":lib"],
    ),
)

operator_libraries(
    name = "operator_qtype_with_value_qtype",
    operator_name = "qtype.with_value_qtype",
    overloads = operator_overload_list(
        hdrs = ["qtype_operator.h"],
        arg_lists = [
            (qtype_type, qtype_type),
        ],
        op_class = "::arolla::WithValueQTypeOp",
        deps = [":lib"],
    ),
)

operator_family(
    name = "operator_seq_at",
    hdrs = ["sequence_operators.h"],
    op_family_class = "::arolla::SequenceAtOpFamily",
    op_family_name = "seq.at",
    deps = [":lib"],
)

operator_family(
    name = "operator_seq_size",
    hdrs = ["sequence_operators.h"],
    op_family_class = "::arolla::SequenceSizeOpFamily",
    op_family_name = "seq.size",
    deps = [":lib"],
)

operator_family(
    name = "operator_seq_slice",
    hdrs = ["sequence_operators.h"],
    op_family_class = "::arolla::SequenceSliceOpFamily",
    op_family_name = "seq.slice",
    deps = [":lib"],
)

operator_family(
    name = "operator_seq_zip",
    hdrs = ["sequence_operators.h"],
    op_family_class = "::arolla::SequenceZipOpFamily",
    op_family_name = "seq.zip",
    deps = [":lib"],
)
