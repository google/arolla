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
