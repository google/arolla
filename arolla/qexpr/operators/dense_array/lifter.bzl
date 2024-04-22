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

"""Utility to register operator libraries with DenseArray types."""

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "is_liftable",
    "meta_type_list",
    "operator_overload",
)

dense_array_edge_type = "::arolla::DenseArrayEdge"
dense_array_scalar_edge_type = "::arolla::DenseArrayGroupScalarEdge"

def make_dense_array_type(t):
    return "::arolla::AsDenseArray<{}>".format(t)

def _make_dense_array_op(t, args):
    # Note: we use NoBitmapOffset=true for performance reasons.
    # To make it work correctly we have to call DenseArray::ForceNoBitmapBitOffset
    # in `array.slice` (on DenseArray) and in `array._as_dense_array` (on Array) operators.
    return "::arolla::DenseArrayLifter<{}, {}, /*NoBitmapOffset=*/true>".format(
        t,
        meta_type_list(args),
    )

def lift_to_dense_array(op):
    """Lifts pointwise operator implementation to support DenseArray types."""
    return operator_overload(
        args = [(make_dense_array_type(a) if is_liftable(a) else a) for a in op.args],
        op_class = _make_dense_array_op(op.op_class, op.args),
        hdrs = op.hdrs + [
            "arolla/qexpr/operators/dense_array/lifter.h",
            "arolla/dense_array/qtype/types.h",
        ],
        deps = op.deps + [
            "//arolla/qexpr/operators/dense_array:lib",
            "//arolla/dense_array/qtype",
        ],
        build_target_groups = ["on_dense_arrays"],
    )

def _make_dense_array_group_op(acc):
    return "::arolla::DenseArrayGroupLifter<{},{},{}>".format(
        acc,
        acc + "::parent_types",
        acc + "::child_types",
    )

def lift_accumulator_to_dense_array(acc_overload, edge):
    """Lifts accumulator to a group_op operator on Dense Arrays.

    Args:
        acc_overload: implementation specification of the wrapped accumulator. Must be
            constructed using accumulator_overload.
        edge: the C++ type of an index edge to use for the operator

    Returns:
        implementation specification for the resulting operator.
    """
    if edge == dense_array_edge_type:
        parent_args = [make_dense_array_type(arg) for arg in acc_overload.parent_args]
    else:
        parent_args = acc_overload.parent_args
    child_args = [make_dense_array_type(arg) for arg in acc_overload.child_args]
    op_args = parent_args + child_args + [edge] + acc_overload.init_args
    return operator_overload(
        args = op_args,
        op_class = _make_dense_array_group_op(acc_overload.acc_class),
        hdrs = acc_overload.hdrs + ["arolla/qexpr/operators/dense_array/group_lifter.h"],
        deps = acc_overload.deps + ["//arolla/qexpr/operators/dense_array:lib"],
        build_target_groups = ["on_dense_arrays"],
    )

def lift_accumulator_to_dense_array_with_edge(acc_overload):
    return lift_accumulator_to_dense_array(acc_overload, dense_array_edge_type)

def lift_accumulator_to_dense_array_with_scalar_edge(acc_overload):
    return lift_accumulator_to_dense_array(acc_overload, dense_array_scalar_edge_type)

dense_array_accumulator_lifters = [
    lift_accumulator_to_dense_array_with_edge,
    lift_accumulator_to_dense_array_with_scalar_edge,
]
