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
    "operator_family",
)
load("//py/arolla/optools:optools.bzl", "arolla_cc_operator_package")

licenses(["notice"])

arolla_cc_operator_package(
    name = "operators",
    arolla_initializer = {"name": "arolla_operators/objects"},
    snapshot = "//py/arolla/objects/operators:operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [
        ":expr",
        ":qexpr",
    ],
)

cc_library(
    name = "expr",
    srcs = ["expr_operators.cc"],
    hdrs = [],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//arolla:internal"],
    deps = [
        "//arolla/expr",
        "//arolla/objects",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
    alwayslink = True,
)

operator_lib_list = [
    # go/keep-sorted start
    ":operator_objects_get_object_attr",
    ":operator_objects_make_object",
    # go/keep-sorted end
]

cc_library(
    name = "qexpr",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    visibility = ["//arolla:internal"],
    deps = operator_lib_list,
)

cc_library(
    name = "qexpr_operators",
    srcs = ["qexpr_operators.cc"],
    hdrs = ["qexpr_operators.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/objects",
        "//arolla/qexpr",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

operator_family(
    name = "operator_objects_make_object",
    hdrs = ["qexpr_operators.h"],
    op_family_class = "::arolla::MakeObjectOperatorFamily",
    op_family_name = "objects.make_object",
    deps = [":qexpr_operators"],
)

operator_family(
    name = "operator_objects_get_object_attr",
    hdrs = ["qexpr_operators.h"],
    op_family_class = "::arolla::GetObjectAttrOperatorFamily",
    op_family_name = "objects.get_object_attr",
    deps = [":qexpr_operators"],
)
