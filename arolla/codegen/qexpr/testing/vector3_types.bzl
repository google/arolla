"""Utility to register operator libraries with Vector3 types."""

load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "operator_overload",
)

def _make_vector3_type(t):
    return "::arolla::testing::Vector3<{}>".format(t)

def _make_vector3_op(t):
    return "::arolla::testing::Vector3LiftedOperatorTraits<{}>".format(t)

def lift_to_vector3(op):
    return operator_overload(
        args = [_make_vector3_type(a) for a in op.args],
        op_class = _make_vector3_op(op.op_class),
        hdrs = op.hdrs + [native.package_name() + "/test_operators.h"],
        deps = op.deps + [":test_operators_lib"],
        build_target_groups = ["on_vector3"],
    )
