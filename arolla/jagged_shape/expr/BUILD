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

load("//py/arolla/optools:optools.bzl", "arolla_cc_operator_package")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

arolla_cc_operator_package(
    name = "expr",
    arolla_initializer = {"name": "arolla_operators/jagged_shape"},
    snapshot = "//py/arolla/jagged_shape/operators:operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = ["//arolla/jagged_shape/qexpr"],
)
