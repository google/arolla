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
load("//py/arolla/dynamic_deps:build_defs.bzl", "arolla_pybind_extension")

package(default_visibility = ["//py/arolla/s11n:__subpackages__"])

licenses(["notice"])

py_library(
    name = "s11n",
    srcs = ["s11n.py"],
    visibility = ["//arolla:internal"],
    deps = [
        ":clib",
        "//arolla/proto:serialization_base_py_proto",
        "//py:python_path",
        "//py/arolla/abc",
        "//py/arolla/s11n/py_object_codec",
    ],
)

arolla_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    pytype_deps = ["//arolla/proto:serialization_base_py_proto"],
    pytype_srcs = ["clib.pyi"],
    deps = [
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization",
        "//arolla/serialization:riegeli",
        "//arolla/serialization_base:base_cc_proto",
        "//arolla/serialization_codecs:all",
        "//py/arolla/abc:pybind11_utils",
        "//py/arolla/py_utils",
        "//py/arolla/s11n/py_object_codec:codec",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
        "@pybind11_protobuf//pybind11_protobuf:native_proto_caster",
    ],
)

py_test(
    name = "s11n_test",
    srcs = ["s11n_test.py"],
    deps = [
        ":s11n",
        "//py:python_path",
        "//py/arolla/abc",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
