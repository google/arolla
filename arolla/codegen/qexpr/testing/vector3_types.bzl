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
