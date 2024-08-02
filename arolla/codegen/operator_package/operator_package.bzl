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

"""Utility for operator package embedding."""

load("//arolla/codegen:utils.bzl", "read_file_function", "render_jinja2_template")

def _unique(names):
    return sorted({k: None for k in names})

def _validate_initializer_name(name):
    return (type(name) == "string" and
            name != "" and all([
        (("A" <= c and c <= "Z") or ("a" <= c and c <= "z") or
         ("0" <= c and c <= "9") or (c in "._/@:"))
        for c in name.elems()
    ]))

def arolla_initializer_spec(
        name = None,
        deps = (),
        reverse_deps = ()):
    """Returns a dict with the arolla initializer spec.

    This macro implements some additional checks and performs normalization.
    Notably, you can use it as:

       arolla_initializer = arolla_initializer_spec(**arolla_initializer)

    Additional information about the Arolla initializers and their parameters
    can be found in `arolla/util/init_arolla.h`.

    Args:
      name: An initializer name; the name should be globally unique or omitted
        for anonymous initializers.
      deps: A list of initializer names that must be executed before this one.
      reverse_deps: A list of initializer names that must be executed after
        this one.

    Returns:
      A dict with the arolla initializer spec.
    """
    result = {}
    if name != None:
        if not _validate_initializer_name(name):
            fail("%s is not a valid initializer name" % repr(name))
        result["name"] = name
    if type(deps) not in ("list", "tuple"):
        fail("'deps' must be list[string]: %s" % repr(type(deps)))
    for dep in deps:
        if not _validate_initializer_name(dep):
            fail("%s is not a valid initializer dependency name" %
                 repr(dep))
    result["deps"] = _unique(deps)
    if type(reverse_deps) not in ("list", "tuple"):
        fail("'reverse_deps' must be list[string]")
    for reverse_dep in reverse_deps:
        if not _validate_initializer_name(reverse_dep):
            fail("%s is not a valid initializer dependency name" %
                 repr(reverse_dep))
    result["reverse_deps"] = _unique(reverse_deps)
    return result

def arolla_cc_embed_operator_package(
        name,
        srcs,
        arolla_initializer,
        deps = (),
        tags = (),
        testonly = False,
        visibility = None):
    """Embeds an operator package to the arolla binary.

    All operators will be registered to the global registry by InitArolla().

    This is a low-level rule. If there is a more specialised one for your
    usecase, please consider using it.

    Args:
      name: the name of the resulting library.
      srcs: a serialized operator package.
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
    if len(srcs) != 1:
        fail("exactly one src value supported", "srcs")
    src = srcs[0]
    src_compressed = "%s_compressed.gz" % name
    native.genrule(
        name = name + "_compressed",
        srcs = [src],
        outs = [src_compressed],
        cmd = "gzip --best --stdout $(SRCS) > $(OUTS)",
        testonly = testonly,
        tags = tags,
        visibility = ["//visibility:private"],
    )
    cc_file_rule = name + "_cc"
    cc_file = name + ".cc"
    arolla_initializer = arolla_initializer_spec(**arolla_initializer)
    render_jinja2_template(
        name = cc_file_rule,
        out = cc_file,
        template = "//arolla/codegen/operator_package:operator_package.cc.jinja2",
        testonly = testonly,
        context = dict(
            build_target = "//{}:{}".format(
                native.package_name(),
                name,
            ),
            arolla_initializer = arolla_initializer,
            operator_package_data = read_file_function(src_compressed),
        ),
        tags = tags,
        visibility = ["//visibility:private"],
    )
    native.cc_library(
        name = name,
        srcs = [cc_file],
        deps = [
            "@com_google_absl//absl/status",
            "@com_google_absl//absl/strings:string_view",
            "@com_google_protobuf//:protobuf_lite",
            "//arolla/codegen/operator_package",
            "//arolla/codegen/operator_package:operator_package_cc_proto",
            "//arolla/util",
        ] + list(deps),
        alwayslink = True,
        testonly = testonly,
        tags = tags,
        visibility = visibility,
    )
