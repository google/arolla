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
    "operator_overload_list",
    "qtype_type",
)

# The operators for the "lazy" type.
load("//py/arolla/optools:optools.bzl", "arolla_cc_operator_package")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

arolla_cc_operator_package(
    name = "operators",
    arolla_initializer = {"name": "arolla_operators/lazy"},
    snapshot = "//py/arolla/lazy:operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [":qexpr"],
)

cc_library(
    name = "qexpr_operators_lib",
    hdrs = ["qexpr_operators_lib.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/lazy",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
    ],
)

operator_libraries(
    name = "qexpr_operator_lazy_get_lazy_qtype",
    operator_name = "lazy.get_lazy_qtype",
    overloads = operator_overload_list(
        hdrs = ["qexpr_operators_lib.h"],
        arg_lists = [(qtype_type,)],
        op_class = "::arolla::GetLazyQTypeOp",
        deps = [":qexpr_operators_lib"],
    ),
)

operator_family(
    name = "qexpr_operator_lazy_get",
    hdrs = ["qexpr_operators_lib.h"],
    op_family_class = "::arolla::LazyGetOpFamily",
    op_family_name = "lazy.get",
    deps = [":qexpr_operators_lib"],
)

cc_library(
    name = "qexpr",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    visibility = ["//visibility:public"],
    deps = [
        ":qexpr_operator_lazy_get",
        ":qexpr_operator_lazy_get_lazy_qtype",
    ],
)
