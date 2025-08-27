# Copyright 2025 Google LLC
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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load(
    "//arolla/codegen/qexpr:register_operator.bzl",
    "operator_libraries",
    "operator_overload_list",
)
load("//py/arolla/optools:optools.bzl", "arolla_cc_operator_package")

licenses(["notice"])

arolla_cc_operator_package(
    name = "operators",
    arolla_initializer = {"name": "arolla_operators/objects"},
    snapshot = "//py/arolla/objects/operators:operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [
        ":qexpr",
    ],
)

operator_lib_list = [
    # go/keep-sorted start
    ":operator_objects_make_object_qtype",
    # go/keep-sorted end
]

cc_library(
    name = "qexpr",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    visibility = ["//visibility:public"],
    deps = operator_lib_list,
)

cc_library(
    name = "qexpr_operators",
    srcs = [],
    hdrs = ["qexpr_operators.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/objects",
        "//arolla/qtype",
    ],
)

operator_libraries(
    name = "operator_objects_make_object_qtype",
    operator_name = "objects.make_object_qtype",
    overloads = operator_overload_list(
        hdrs = ["qexpr_operators.h"],
        arg_lists = [()],
        op_class = "::arolla::MakeObjectQTypeOp",
        deps = [":qexpr_operators"],
    ),
)
