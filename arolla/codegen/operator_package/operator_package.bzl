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

def arolla_cc_embed_operator_package(
        name,
        srcs,
        priority_name,
        tags = (),
        testonly = False,
        visibility = None,
        deps = ()):
    """Embeds an operator package to the arolla binary.

    All operators will be registered to the global registry by InitArolla().

    This is a low-level rule. If there is a more specialised one for your
    usecase, please consider using it.

    Args:
      name: the name of the resulting library.
      srcs: a serialized operator package.
      priority_name: initializer priority (see util/init_arolla.h).
      tags: tags.
      testonly: if True, only testonly targets (such as tests) can depend on
        this target.
      visibility: target's visibility.
      deps: extra dependencies, e.g. serialization codecs to load the package.
    """
    if visibility == None:
        kwargs = {}
    else:
        kwargs = {"visibility": visibility}
    if len(srcs) != 1:
        fail("exactly one src value supported", "srcs")
    src = srcs[0]
    src_compressed = src + ".gz"
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
            initializer_priority = priority_name,
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
            "//arolla/codegen/operator_package:load_operator_package",
            "//arolla/codegen/operator_package:operator_package_cc_proto",
            "//arolla/util",
        ] + list(deps),
        alwayslink = True,
        testonly = testonly,
        tags = tags,
        **kwargs
    )
