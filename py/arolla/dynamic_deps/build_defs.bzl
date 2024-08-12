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

load("@bazel_skylib//rules:write_file.bzl", "write_file")

def arolla_pybind_extension(
        name,
        srcs,
        deps = (),
        dynamic_deps = (),  # buildifier: disable=unused-variable
        pytype_deps = (),
        pytype_srcs = (),
        tags = (),
        testonly = False,
        visibility = None):
    """Builds an Arolla PyBind11 extension module.

    Extensions created using this macro share the same instances of
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
        dynamic_deps = list(dynamic_deps) + [
            "//py/arolla/dynamic_deps:arolla_py_abc_so",
            "//py/arolla/dynamic_deps:arolla_so",
            "//py/arolla/dynamic_deps:base_so",
        ],
        tags = list(tags),
        testonly = testonly,
        visibility = visibility,
    )

def arolla_py_cc_deps(
        name,
        deps,
        dynamic_deps = (),
        testonly = False,
        visibility = None):
    """Python extension that dynamically loads `cc_library`es.

    This macro is intended as a helper that dynamically loads components such
    as Arolla types, serialization codecs, and operators. These components are
    usually implemented as cc_library() targets.

    The resulting python extension must be manually loaded from a .py file.

    Extensions created using this macro share the same instances of
    the following libraries:

      * py/arolla/operators (!)
      * py/arolla:py_abc
      * Arolla (particularly, the operator registry)
      * Abseil
      * Protobuf

    IMPORTANT: It is crucial that each cc_library() is provided by at most one
    Python extension. Failing to do so can easily lead to collisions between
    dynamically loaded symbols at runtime!

    As a rule of thumb, if a cc_library() is wrapped using arolla_py_cc_deps(),
    no other targets in the Python API should directly depend on it.

    In general, the same rule applies to implicit dependencies. However, when
    there is a conflict between implicit dependencies, the conflicts need to be
    investigated on a case-by-case basis.
    """
    write_file(
        name = "gen_{}_cc".format(name),
        out = "{}.cc".format(name),
        content = [
            '#include "pybind11/pybind11.h"',
            "PYBIND11_MODULE({}, m) {{ }}".format(name),
        ],
        testonly = testonly,
        visibility = ["//visibility:private"],
    )
    arolla_pybind_extension(
        name = name,
        srcs = ["{}.cc".format(name)],
        deps = deps,
        dynamic_deps = list(dynamic_deps) + [
            "//py/arolla/dynamic_deps:arolla_standard_operators_so",
        ],
        testonly = testonly,
        visibility = visibility,
    )
