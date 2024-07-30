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

"""Utility for operator package generation."""

load(
    "//arolla/codegen/operator_package:operator_package.bzl",
    "arolla_cc_embed_operator_package",
    "arolla_initializer_spec",
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
        "//arolla/expr/operators/all",
        "//arolla/serialization_codecs/generic/decoders",
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
