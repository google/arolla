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
      srcs: serialized operator packages.
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
            data_files = {f: read_file_function(f) for f in srcs},
        ),
        tags = tags,
        visibility = ["//visibility:private"],
    )
    native.cc_library(
        name = name,
        srcs = [
            cc_file,
        ],
        deps = [
            "@com_google_absl//absl/status",
            "@com_google_absl//absl/strings",
            "//arolla/expr/operator_loader",
            "//arolla/util",
        ] + list(deps),
        alwayslink = True,
        testonly = testonly,
        tags = tags,
        **kwargs
    )
