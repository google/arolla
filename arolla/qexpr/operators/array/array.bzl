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

"""Utility to register operator libraries with Array types."""

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "decay_dont_lift",
    "is_liftable",
    "meta_type_list",
    "operator_overload",
)

array_edge_type = "::arolla::ArrayEdge"
array_scalar_edge_type = "::arolla::ArrayGroupScalarEdge"

array_hdrs = [
    "arolla/array/qtype/types.h",
    "arolla/qexpr/lift_to_optional_operator.h",
    "arolla/qexpr/operators/dense_array/lifter.h",
    "arolla/qexpr/operators/array/lifter.h",
]

array_deps = [
    "//arolla/array",
    "//arolla/array/qtype",
    "//arolla/qexpr/operators/dense_array:lib",
    "//arolla/qexpr/operators/array:lib",
]

def make_array_type(t):
    return "::arolla::AsArray<{}>".format(decay_dont_lift(t))

def make_array_op(dense_op, fn, liftable_args):
    optional_op = "::arolla::OptionalLiftedOperator<{}, {}>".format(
        fn,
        meta_type_list(liftable_args),
    )
    return "::arolla::ArrayPointwiseLifterOnDenseOp<{}, {}, {}>".format(
        dense_op,
        optional_op,
        meta_type_list(liftable_args),
    )

def lift_to_array(op):
    """Lifts pointwise operator implementation to support Array types."""
    array_op = "::arolla::ArrayPointwiseLifter<{}, {}>".format(
        op.op_class,
        meta_type_list(op.args),
    )
    return operator_overload(
        args = [(make_array_type(a) if is_liftable(a) else a) for a in op.args],
        op_class = array_op,
        hdrs = op.hdrs + array_hdrs,
        deps = op.deps + array_deps,
        build_target_groups = ["on_arrays"],
    )

def _make_array_group_op(acc):
    return "::arolla::ArrayGroupLifter<{},{},{}>".format(
        acc,
        acc + "::parent_types",
        acc + "::child_types",
    )

def lift_accumulator_to_array(acc_overload, edge):
    """Lifts accumulator to a group_op operator on Arrays.

    Args:
        acc_overload: implementation specification of the wrapped accumulator. Must be
            constructed using accumulator_overload.
        edge: the C++ type of an index edge to use for the operator

    Returns:
        implementation specification for the resulting operator.
    """
    if edge == array_edge_type:
        parent_args = [make_array_type(arg) for arg in acc_overload.parent_args]
    else:
        parent_args = acc_overload.parent_args
    child_args = [make_array_type(arg) for arg in acc_overload.child_args]
    op_args = parent_args + child_args + [edge] + acc_overload.init_args
    return operator_overload(
        args = op_args,
        op_class = _make_array_group_op(acc_overload.acc_class),
        hdrs = acc_overload.hdrs + ["arolla/qexpr/operators/array/lifter.h"],
        deps = acc_overload.deps + ["//arolla/qexpr/operators/array:lib"],
        build_target_groups = ["on_arrays"],
    )

def lift_accumulator_to_array_with_edge(acc_overload):
    return lift_accumulator_to_array(acc_overload, array_edge_type)

def lift_accumulator_to_array_with_scalar_edge(acc_overload):
    return lift_accumulator_to_array(acc_overload, array_scalar_edge_type)

array_accumulator_lifters = [
    lift_accumulator_to_array_with_edge,
    lift_accumulator_to_array_with_scalar_edge,
]
