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

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")

licenses(["notice"])

py_library(
    name = "objects",
    srcs = ["objects.py"],
    strict_deps = False,
    visibility = ["//visibility:public"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/arolla",
        "//py/arolla/objects/operators:binding_policies",
    ],
)

py_test(
    name = "objects_test",
    srcs = ["objects_test.py"],
    strict_deps = False,
    deps = [
        ":objects",
        "//py:python_path",
        "//py/arolla",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "s11n_test",
    srcs = ["s11n_test.py"],
    strict_deps = False,
    deps = [
        ":objects",
        "//arolla/objects/s11n:codec_py_proto",
        "//arolla/serialization_codecs/generic:scalar_codec_py_proto",
        "//py:python_path",
        "//py/arolla/s11n/testing:codec_test_case",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

arolla_py_cc_deps(
    name = "clib",
    dynamic_deps = ["//py/arolla/dynamic_deps:objects_so"],
    deps = [
        "//arolla/objects/operators",
        "//arolla/objects/s11n",
    ],
)
