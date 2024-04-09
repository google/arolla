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

package(default_visibility = ["//py/arolla/types:__subpackages__"])

licenses(["notice"])

py_library(
    name = "types",
    srcs = [
        "types.py",
    ],
    visibility = ["//arolla:internal"],
    deps = [
        "//py/arolla/abc",
        "//py/arolla/types/qtype",
        "//py/arolla/types/qvalue",
        "//py/arolla/types/s11n",
        "//py/arolla/types/s11n:py_object_pickle_codec",
        "//py/arolla/types/s11n:py_object_reference_codec",
        "//py/arolla/types/s11n:registered_py_object_codecs",
    ],
)
