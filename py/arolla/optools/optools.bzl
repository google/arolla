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

"""Utility for operator package generation."""

load("//arolla/codegen:utils.bzl", "arolla_repo_dep", "bash_escape")
load(
    "//arolla/codegen/operator_package:operator_package.bzl",
    "arolla_cc_embed_operator_package",
    "arolla_initializer_spec",
)
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:py_binary.bzl", "py_binary")

def arolla_operator_package_snapshot(
        name,
        srcs = (),
        tags = (),
        preimports = (),
        imports = (),
        deps = (),
        env = {},
        testonly = False,
        visibility = None):
    """Creates an operator package snapshot.

    This rule dumps operator definitions from .py to an operator package protobuf.

    Args:
      name: the name of the resulting library.
      tags: tags.
      imports: a list of python modules providing the new operator declarations.
      preimports: a list of python modules that should be imported before
        the operator declarations. The main purpose is to pre-load other operator
        libraries and serialization codecs.
      deps: a list of dependencies, primarily intended to serve the needs of
        `imports` and `preimports`.
      env: a dict of environment variables to pass to the genrule.
      testonly: if True, only testonly targets (such as tests) can depend on
        this target.
      visibility: target's visibility.
    """
    gen_rule_name = name + "_gen_operator_package"
    exec_rule_name = name + "_exec_gen_operator_package"
    py_binary(
        name = gen_rule_name,
        main = arolla_repo_dep("//py/arolla/optools:gen_operator_package.py"),
        srcs = [arolla_repo_dep("//py/arolla/optools:gen_operator_package.py")],
        strict_deps = False,
        deps = [arolla_repo_dep("//py/arolla/optools:gen_operator_package")] + list(srcs) + list(deps),
        tags = tags,
        testonly = testonly,
        visibility = ["//visibility:private"],
    )
    env = {"LC_ALL": "C.UTF-8"} | env
    native.genrule(
        name = exec_rule_name,
        srcs = srcs,
        outs = [name],
        cmd = ("".join(["{}={} ".format(k, bash_escape(v)) for k, v in env.items()]) +
               "$(execpath {})".format(gen_rule_name) +
               " --name={}".format(name) +
               " --output_file=$(execpath {})".format(name) +
               "".join(
                   [" --preimports={}".format(m) for m in preimports],
               ) +
               "".join(
                   [" --imports={}".format(m) for m in imports],
               ) +
               "".join(
                   [" --import_modules=$(execpath {})".format(m) for m in srcs],
               )),
        tools = [gen_rule_name],
        tags = tags,
        testonly = testonly,
        visibility = visibility,
    )

def arolla_cc_operator_package(
        name,
        snapshot,
        arolla_initializer = None,
        deps = (),
        tags = (),
        testonly = False,
        visibility = None):
    """Creates a cc_library that loads an operator package snapshot.

    This is a higher-level wrapper for `arolla_cc_embed_operator_package()`,
    that automatically adds some common dependencies, in particular,
    the standard operator library and generic de-serialization codecs.

    Args:
      name: the name of the resulting library.
      snapshot: a operator package snapshot.
      arolla_initializer: the initializer settings specified in the format
        `dict(name='<name>', deps=['<dep>', ...], reverse_deps=['<dep>', ...])`
        (see `arolla_initializer_spec` or `arolla/util/init_arolla.h` for more
        information).
      deps: extra dependencies, e.g. serialization codecs.
      tags: tags.
      testonly: if True, only testonly targets (such as tests) can depend on
        this target.
      visibility: target's visibility.
    """
    arolla_initializer = arolla_initializer_spec(**(arolla_initializer or {}))
    arolla_initializer["deps"].append("arolla_operators/standard")
    deps = list(deps) + [
        arolla_repo_dep("//arolla/expr/operators/all"),
        arolla_repo_dep("//arolla/serialization_codecs/generic/decoders"),
    ]
    arolla_cc_embed_operator_package(
        name = name,
        srcs = [snapshot],
        arolla_initializer = arolla_initializer,
        tags = tags,
        testonly = testonly,
        deps = deps,
        visibility = visibility,
    )
    cc_test(
        name = name + "_smoke_test",
        deps = [
            ":" + name,
            arolla_repo_dep("//py/arolla/optools:arolla_cc_operator_package_smoke_test"),
        ],
        tags = tags,
    )
