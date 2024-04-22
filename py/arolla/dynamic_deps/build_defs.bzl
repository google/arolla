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

"""This module contains build rules for Arolla Python extensions.
"""

load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")

def arolla_pybind_extension(
        name,
        srcs,
        deps = (),
        pytype_deps = (),
        pytype_srcs = (),
        tags = (),
        testonly = False,
        visibility = ()):
    """Builds an Arolla PyBind11 extension module.

    All extensions created using this rule share the same instance of
    the following libraries:

      * py/arolla:py_abc
      * Arolla (particularly, the operator registry)
      * Abseil
      * Protobuf

    These libraries are designed to function as singletons and may operate
    incorrectly if each Python extension statically links to a "private" copy
    of the library (even when the symbols are hidden).
    """
    # NOTE: We currently don't support: `pytype_deps=`, `pytype_srcs=`.
    pybind_extension(
        name = name,
        srcs = list(srcs),
        deps = list(deps),
        dynamic_deps = [
            "//py/arolla/dynamic_deps:arolla_so",
            "//py/arolla/dynamic_deps:base_so",
            "//py/arolla/dynamic_deps:py_abc_so",
        ],
        tags = list(tags),
        testonly = testonly,
        visibility = list(visibility),
    )
