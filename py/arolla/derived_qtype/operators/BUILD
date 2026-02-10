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

load("@rules_python//python:defs.bzl", "py_library")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")
load("//py/arolla/optools:optools.bzl", "arolla_operator_package_snapshot")

licenses(["notice"])

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = ["arolla.derived_qtype.operators.derived_qtype"],
    preimports = ["arolla.derived_qtype.operators.operators_bootstrap_clib"],
    visibility = ["//arolla:internal"],
    deps = [
        ":derived_qtype",
        ":operators_bootstrap_clib",
    ],
)

py_library(
    name = "derived_qtype",
    srcs = ["derived_qtype.py"],
    strict_deps = False,
    deps = [
        "//py:python_path",
        "//py/arolla",
    ],
)

# Note: This target partially duplicates py/arolla/derived_qtype:clib.
# However, this Python extension is used exclusively for the operator package declaration.
# Both extensions are never imported simultaneously!
arolla_py_cc_deps(
    name = "operators_bootstrap_clib",
    deps = ["//arolla/derived_qtype/operators:bootstrap"],
)
