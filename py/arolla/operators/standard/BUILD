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

load("@rules_python//python:defs.bzl", "py_library")
load("//py/arolla/optools:optools.bzl", "arolla_operator_package_snapshot")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = [
        "arolla.operators.standard.array",
        "arolla.operators.standard.bitwise",
        "arolla.operators.standard.bool",
        "arolla.operators.standard.core",
        "arolla.operators.standard.dict",
        "arolla.operators.standard.edge",
        "arolla.operators.standard.experimental",
        "arolla.operators.standard.math",
        "arolla.operators.standard.namedtuple",
        "arolla.operators.standard.qtype",
        "arolla.operators.standard.random",
        "arolla.operators.standard.seq",
        "arolla.operators.standard.strings",
    ],
    visibility = ["//arolla:internal"],
    deps = [
        ":array",
        ":bitwise",
        ":bool",
        ":core",
        ":dict",
        ":edge",
        ":experimental",
        ":math",
        ":namedtuple",
        ":qtype",
        ":random",
        ":seq",
        ":strings",
    ],
)

py_library(
    name = "array",
    srcs = ["array.py"],
    deps = [
        ":core",
        ":qtype",
        ":seq",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "bitwise",
    srcs = [
        "bitwise.py",
    ],
    deps = [
        ":qtype",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "bool",
    srcs = [
        "bool.py",
    ],
    deps = [
        ":edge",
        ":qtype",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "core",
    srcs = [
        "core.py",
    ],
    deps = [
        ":qtype",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "dict",
    srcs = [
        "dict.py",
    ],
    srcs_version = "PY3",
    deps = [
        ":array",
        ":core",
        ":qtype",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "edge",
    srcs = [
        "edge.py",
    ],
    deps = [
        ":array",
        ":core",
        ":dict",
        ":qtype",
        ":seq",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "experimental",
    srcs = [
        "experimental.py",
    ],
    deps = [
        ":array",
        ":core",
        ":math",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "math",
    srcs = [
        "math.py",
    ],
    deps = [
        ":array",
        ":core",
        ":edge",
        ":qtype",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "namedtuple",
    srcs = [
        "namedtuple.py",
    ],
    deps = [
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "qtype",
    srcs = [
        "qtype.py",
    ],
    deps = ["//py/arolla:arolla_without_predeclared_operators"],
)

py_library(
    name = "seq",
    srcs = [
        "seq.py",
    ],
    deps = [
        ":core",
        ":qtype",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "strings",
    srcs = [
        "strings.py",
    ],
    deps = [
        ":core",
        ":qtype",
        ":seq",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)

py_library(
    name = "random",
    srcs = [
        "random.py",
    ],
    deps = [
        ":array",
        ":core",
        ":edge",
        ":math",
        ":qtype",
        ":strings",
        "//py/arolla:arolla_without_predeclared_operators",
    ],
)
