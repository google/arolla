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

"""Utility to generate symbol registration libraries."""

load(
    "//arolla/codegen:utils.bzl",
    "render_jinja2_template",
)

integral_types = ["int32_t", "int64_t"]
float_types = ["float", "double"]
numeric_types = integral_types + float_types
bool_type = "bool"
uint64_type = "uint64_t"
bytes_type = "::arolla::Bytes"
text_type = "::arolla::Text"
unit_type = "::arolla::Unit"
qtype_type = "::arolla::QTypePtr"
string_types = [bytes_type, text_type]

scalar_types = numeric_types + [
    bytes_type,
    text_type,
    bool_type,
    unit_type,
    uint64_type,
]

dict_key_types = integral_types + [
    bytes_type,
    text_type,
    bool_type,
    uint64_type,
]

dict_types = [[
    k,
    "::arolla::KeyToRowDict<{}>".format(k),
] for k in dict_key_types]

def dont_lift(t):
    """Wrap argument type into DoNotLiftTag."""
    return "::arolla::DoNotLiftTag<{}>".format(t)

def decay_dont_lift(t):
    """Unwrap argument type from DoNotLiftTag."""
    prefix = "::arolla::DoNotLiftTag<"
    return t[len(prefix):-1] if t.startswith(prefix) and t.endswith(">") else t

def is_liftable(t):
    """Returns true iff argument is not wrapped with DoNotLiftTag."""
    return not (t.startswith("::arolla::DoNotLiftTag<") and t.endswith(">"))

def _is_optional_type(t):
    return t.startswith("::arolla::OptionalValue<") or t == "::arolla::OptionalUnit"

def identity(t):
    """Identity function to use in operator definitions."""
    return t

def make_optional_type(t):
    """Constructs an optional version of the given type.

    Args:
        t: input type.
    Returns:
        optional type corresponding to type t.
    """
    t = decay_dont_lift(t)
    if _is_optional_type(t):
        return t
    if t == unit_type:
        return "::arolla::OptionalUnit"
    return "::arolla::OptionalValue<{}>".format(t)

def make_optional_types(types):
    return [make_optional_type(t) for t in types]

def unary_args(types):
    """Converts a list of types to a list of input type lists for unary operator.

    Args:
        types: list of types to convert.
    Returns:
        list of lists of possible argument types.
    """
    return [[t] for t in types]

def binary_args(types):
    """Converts a list of types to a list of input type lists for binary operator.

    Args:
        types: list of types to convert.
    Returns:
        list of lists of possible argument types.
    """
    return [[t, t] for t in types]

def n_ary_args(n, types):
    """Converts a list of types to a list of input type lists for n-ary operator.

    Args:
        n: operator arity.
        types: list of types to convert.
    Returns:
        list of lists of possible argument types.
    """
    return [[t] * n for t in types]

_TYPE_SHORTCUTS = [
    ("int32_t", "i32"),
    ("int64_t", "i64"),
    ("float", "f32"),
    ("double", "f64"),
    ("std::vector", "vector"),
    ("AsDenseArray<::arolla::OptionalValue<", "DenseArray<"),
    ("AsDenseArray<::arolla::OptionalUnit", "DenseArray<Unit"),
    ("AsDenseArray<", "DenseArray<"),
    ("AsArray<::arolla::OptionalValue<", "QBlock<"),
    ("AsArray<::arolla::OptionalUnit", "QBlock<Unit"),
    ("AsArray<", "QBlock<"),
    ("DenseArray", "darray"),
    ("::arolla::OptionalValue<", "optional_"),  # skip "_of_" infix
    ("::arolla::DoNotLiftTag<", ""),
    ("::arolla::", "_"),
    ("arolla::", "_"),
]

def registered_operator_lib(main_rule, arg_types):
    """Constructs a rule name for operator registration library.

    Args:
      main_rule: rule defined by register_simple_operator_libraries call.
      arg_types: operator argument types

    Returns:
      name for a build rule with operator registration with the provided
      arguments.

    Example:
      deps = [
        registered_operator_lib(":registered_qblock_add",
                                ["Array<int64_t>", "Array<int64_t>"])
      ]
      will create dependency on
      :registered_add_qblock_of_i64_and_qblock_of_i64
    """
    suffix = ",".join(arg_types)
    for full, short in _TYPE_SHORTCUTS:
        suffix = suffix.replace(full, short)
    suffix = (suffix
        .lower()
        .replace(" ", "")
        .replace("::", "_")
        .replace("<", "_of_")
        .replace(",", "_and_")
        .replace(">", ""))
    suffix = "_".join([w for w in suffix.split("_") if w])  # remove duplicated "_"
    return main_rule + "_" + suffix

def _header_fullpath(hdr):
    if "/" not in hdr:
        return "{}/{}".format(native.package_name(), hdr)
    else:
        return hdr

def _dep_fullpath(dep):
    if dep.startswith(":"):
        return "//{}{}".format(native.package_name(), dep)
    else:
        return dep

def _unique(items):
    return depset(items).to_list()

def _generate_operators_lib(
        name,
        operator_name,
        overloads,
        is_individual_operator,
        tags,
        **kwargs):
    cc_file = name + ".cc"
    cc_file_rule = cc_file.replace(".", "_")

    hdrs = _unique([h for op in overloads for h in op.hdrs])
    deps = _unique([d for op in overloads for d in op.deps])

    render_jinja2_template(
        name = cc_file_rule,
        out = cc_file,
        template = "//arolla/codegen/qexpr:simple_operator.cc.jinja2",
        testonly = kwargs.get("testonly", 0),
        context = dict(
            op_name = operator_name,
            build_target = "//{}:{}".format(native.package_name(), name),
            overloads = overloads,
            hdrs = hdrs,
            deps = deps,
            is_individual_operator = is_individual_operator,
        ),
        tags = tags,
    )

    native.cc_library(
        name = name,
        srcs = [cc_file],
        deps = _unique(deps + [
            "//arolla/qexpr",
            "//arolla/qtype",
            "//arolla/util",
        ]),
        alwayslink = 1,
        tags = tags,
        **kwargs
    )

def operator_libraries(
        name,
        operator_name,
        overloads,
        **kwargs):
    """Creates libraries for operator registration in OperatorRegistry.

    The rule is configured by a list of operator instance specifications. It
    generates the following libraries:
      1) one library that registers an operator for each operator instance.
         The name pattern is @name + argument types. For example,
         operator_add_i32_and_i32.
      2) a "wildcard" library that includes registrations of all the instances.
         The name is just @name.
      3) one library for each overload group (e.g. plain, on_optional,
         on_dense_array). The name pattern is @name + "_" + overload_group. For
         example, operator_add_on_optional.
      4) a metadata library used by codegen. The name pattern is @name +
         "_metadata".

    By default the generated operator libraries are built without debug
    information. To enable it pass
    --//arolla/codegen/qexpr:include_operators_debug_info=true
    flag to blaze. To enable it selectively for one operator, add
    `copts = ["-g"]` to the build rule.

    Args:
        name: the name of the resulting library (also a prefix for the
          single-type sublibraries).
        operator_name: name for the resulting operator, in a form
          'namespace.nested_namespace.name'.
        overloads: list of `operator_overload` specifications. See
          `operator_overload` documentation for more details.
        **kwargs: other arguments to forward to the generated cc_library rules.

    Example:
      In arithmetic.h:

        namespace my_project {
        struct AddOp {
          template <typename T>
          T operator()(T arg1, T arg2) const {
            return arg1 + arg2;
          }
        };
        } // namespace my_project

      In BUILD file:

        load(
            "//arolla/codegen/qexpr:register_operator.bzl",
            "binary_args",
            "numeric_types",
            "operator_libraries",
            "operator_overload_list",
            "with_lifted_by",
            "lift_to_optional",
        )

        cc_library(
            name = "arithmetic",
            traits_hdrs = ["arithmetic.h"],
        )

        operator_libraries(
            name = "operator_add",
            operator_name = "my_project.add",
            overloads = with_lifted_by(
                [lift_to_optional],
                operator_overload_list(
                    arg_lists = binary_args(numeric_types),
                    op_class = "::my_project::AddOp",
                    hdrs = ["arithmetic.h"],
                    deps = [":arithmetic"],
                ),
            ),
            operator = "my_project.add",
        )

      This will generate libraries:
            --- individual libraries ---
            operator_add — includes all other libraries
            operator_add_i32_and_i32
            operator_add_i64_and_i64
            operator_add_f32_and_f32
            ... (for all numeric_types)
            operator_add_optional_i16_and_optional_i16
            ... (for all optional numeric_types)
            --- operator groups ---
            operator_add_plain - includes all plain libraries
            operator_add_on_optionals - include all optional libraries
            operator_add — includes all other libraries
            --- operator metadata (used by Codegen) ---
            operator_add_metadata
    """

    # Disable debug information generation for the generated code. Explicit
    # -g* specification in copts overrides this.
    kwargs["copts"] = select({
        "//arolla/codegen/qexpr:include_operators_debug_info_enabled": ["-g1"],
        "//conditions:default": ["-g0"],
    }) + kwargs.get("copts", [])

    overload_groups = dict()
    for op in overloads:
        if len(op.build_target_groups) > 1:
            fail("more than one build target group is not allowed")
        for group in (op.build_target_groups or ["plain"]):
            overload_groups[group] = overload_groups.get(group, []) + [op]

    tags = []
    if "tags" in kwargs:
        tags = kwargs["tags"]
        kwargs.pop("tags")

    for op in overloads:
        _generate_operators_lib(
            name = registered_operator_lib(name, op.args),
            operator_name = operator_name,
            overloads = [op],
            is_individual_operator = True,
            # Disable build test on presubmit. The individual operator
            # libraries are still tested if a unit tests depends on them
            # directly (e.g. see testing/ directory).
            tags = tags + ["notap"],
            **kwargs
        )

    group_libs = []
    for group, ops in overload_groups.items():
        lib_name = "{}_{}".format(name, group)
        group_libs.append(":{}".format(lib_name))
        _generate_operators_lib(
            name = lib_name,
            operator_name = operator_name,
            overloads = ops,
            is_individual_operator = False,
            # Disable build test on presubmit. The individual operator
            # libraries are still tested if a unit tests depends on them
            # directly (e.g. see testing/ directory).
            tags = tags + ["notap"],
            **kwargs
        )

    _generate_metadata_lib(name, operator_name, overloads, **kwargs)

    native.cc_library(
        name = name,
        deps = group_libs,
        alwayslink = 1,
        tags = tags,
        **kwargs
    )

def _generate_metadata_lib(name, op_name, overloads, **kwargs):
    """Generates a library that registeres metadatas for all simple operator overloads.

    Returns a name of the generated library.
    """

    lib_name = name + "_metadata"
    cc_file = lib_name + ".cc"
    cc_file_rule = cc_file.replace(".", "_")

    # We don't need actual operator classes, but the headers may also contain
    # QTypeTraits definitions for the operators' argument types.
    all_hdrs = _unique([h for op in overloads for h in op.hdrs])
    context = dict(
        build_target = cc_file_rule,
        hdrs = all_hdrs,
        overloads = [],
    )

    for op in overloads:
        context["overloads"].append(dict(
            build_target = "//{}:{}".format(
                native.package_name(),
                registered_operator_lib(name, op.args),
            ),
            op_name = op_name,
            op_class = op.op_class,
            hdrs = op.hdrs,
            deps = op.deps,
            arg_types = [decay_dont_lift(arg) for arg in op.args],
            arg_as_function_ids = op.arg_as_function_ids,
        ))

    render_jinja2_template(
        name = cc_file_rule,
        out = cc_file,
        template = "//arolla/codegen/qexpr:simple_operator_metadata.cc.jinja2",
        testonly = kwargs.get("testonly", 0),
        context = context,
    )

    all_deps = _unique([d for op in overloads for d in op.deps])
    native.cc_library(
        name = lib_name,
        srcs = [cc_file],
        deps = _unique(all_deps + [
            "//arolla/qexpr",
            "//arolla/qtype",
            "//arolla/util",
        ]),
        alwayslink = 1,
        **kwargs
    )

def operator_family(
        name,
        op_family_name,
        op_family_class,
        hdrs,
        deps,
        **kwargs):
    """Creates a library for OperatorFamily registration in OperatorRegistry.

    Args:
        name: the name of the resulting library.
        op_family_name: name for the resulting operator family, in a form
          'namespace.nested_namespace.name'.
        op_family_class: the fully-qualified name of OperatorFamily subclass.
        hdrs: headers required to compile op_family_class.
        deps: build rules that include hdrs.
        **kwargs: other arguments to forward to the generated cc_library rule.
    """

    cc_file = name + ".cc"
    cc_file_rule = cc_file.replace(".", "_")

    render_jinja2_template(
        name = cc_file_rule,
        out = cc_file,
        template = "//arolla/codegen/qexpr:operator_family_registration.cc.jinja2",
        testonly = kwargs.get("testonly", 0),
        context = dict(
            op_family_name = op_family_name,
            build_target = "//{}:{}".format(native.package_name(), name),
            op_family_class = op_family_class,
            hdrs = _unique([_header_fullpath(h) for h in hdrs]),
            deps = _unique([_dep_fullpath(d) for d in deps]),
        ),
    )

    native.cc_library(
        name = name,
        srcs = [cc_file],
        deps = _unique(deps + [
            "//arolla/qexpr",
            "//arolla/util",
        ]),
        alwayslink = 1,
        **kwargs
    )

    metadata_cc_file = name + "_metadata.cc"
    metadata_cc_file_rule = metadata_cc_file.replace(".", "_")

    render_jinja2_template(
        name = metadata_cc_file_rule,
        out = metadata_cc_file,
        template = "//arolla/codegen/qexpr:operator_family_metadata.cc.jinja2",
        testonly = kwargs.get("testonly", 0),
        context = dict(
            op_family_name = op_family_name,
            build_target = "//{}:{}".format(native.package_name(), name),
            op_family_class = op_family_class,
            hdrs = _unique([_header_fullpath(h) for h in hdrs]),
            deps = _unique([_dep_fullpath(d) for d in deps]),
        ),
    )

    native.cc_library(
        name = name + "_metadata",
        srcs = [metadata_cc_file],
        deps = _unique(deps + [
            "//arolla/qexpr",
            "//arolla/util",
        ]),
        alwayslink = 1,
        **kwargs
    )

def operator_overload_list(
        op_class,
        arg_lists,
        hdrs,
        deps,
        build_target_groups = None,
        arg_as_function_ids = None):
    """Constructs a list of operator instance specifications.

    The function is an equivalent for calling operator_overload for every
    argument list in arg_lists.

    Args:
        op_class: the fully-qualified name of operator functor class.
            If the operator is cheaper than a single conditional jump, then to improve performance
            add "using run_on_missing = std::true_type;" to the functor definition.
        arg_lists: list of possible argument specifications. Each specification
            is a list of C++ types, one per argument. Consider using predefined
            constants numeric_types, float_types, etc, and functions unary_args,
            binary_args. Use `dont_lift(arg)` in order to mark argument
            to be left as scalar.
        hdrs: additional headers required to compile op_class.
        deps: build rules that include hdrs.
        build_target_groups: optional list of additional build target names
            for the generated overloads. All the targets with the same group will
            be combined into a special ":{library_name}_{group_name}" library.
            Overloads without `build_target_groups` will be put into "plain"
            group by default.

            For example, if an overload for ":operator_add" has "on_optionals"
            and "on_i32" build_target_groups, then additional build targets
            ":operator_add_on_optionals" and ":operator_add_on_i32" grouping
            the corresponding overloads will be generated.
        arg_as_function_ids: optional list of argument ids that can be passed
                as function returning the value.

    Returns:
        List of operator instance specification structs.
    """
    return [
        operator_overload(
            op_class = op_class,
            args = args,
            hdrs = hdrs,
            deps = deps,
            build_target_groups = build_target_groups,
            arg_as_function_ids = arg_as_function_ids,
        )
        for args in arg_lists
    ]

def operator_overload(op_class, args, hdrs, deps, build_target_groups = None, arg_as_function_ids = None):
    """Constructs operator instance specification.

    Args:
        op_class: the fully-qualified name of operator functor class.
            If the operator is cheaper than a single conditional jump, then to improve performance
            add "using run_on_missing = std::true_type;" to the functor definition.
        args: list of C++ types, one per argument. Consider using predefined
            constants numeric_types, float_types, etc, and functions unary_args,
            binary_args. Use `dont_lift(arg)` in order to mark argument
            to be left as scalar.
        hdrs: additional headers required to compile op_class.
        deps: build rules that include hdrs.
        build_target_groups: optional list of additional build target names
            for the generated overloads. All the targets with the same group will
            be combined into a special ":{library_name}_{group_name}" library.
            Overloads without `build_target_groups` will be put into "plain"
            group by default.

            For example, if an overload for ":operator_add" has "on_optionals"
            and "on_i32" build_target_groups, then additional build targets
            ":operator_add_on_optionals" and ":operator_add_on_i32" grouping
            the corresponding overloads will be generated.
        arg_as_function_ids: optional list of argument ids that can be passed
                as function returning the value.

    Returns:
        Operator implementation specification struct.
    """
    hdr_fullpaths = _unique([_header_fullpath(h) for h in hdrs])
    dep_fullpaths = _unique([_dep_fullpath(d) for d in deps])

    if not build_target_groups:
        build_target_groups = ("plain",)
    if not arg_as_function_ids:
        arg_as_function_ids = []

    return struct(
        args = args,
        op_class = op_class,
        hdrs = hdr_fullpaths,
        deps = dep_fullpaths,
        build_target_groups = tuple(build_target_groups),
        arg_as_function_ids = arg_as_function_ids,
    )

def accumulator_overload(
        acc_class,
        hdrs,
        deps,
        parent_args = [],
        child_args = [],
        init_args = [],
        build_target_groups = None):
    """Constructs accumulator instance specification.

    Args:
        acc_class: the fully-qualified name of the accumulator class
            (qexpr/aggregation_ops_interface.h ::arolla::Accumulator)
        parent_args: list of C++ types, one per argument, for the parent
            scalar types that the accumulator consumes.
        child_args: list of C++ types, one per argument, for the child
            scalar types that the accumulator consumes.
        init_args: list of C++ types, one per argument, of the arguments with which
            to initialize the accumulator
        hdrs: additional headers required to compile op_class.
        deps: build rules that include hdrs.
        build_target_groups: optional list of additional build target names
            for the generated overloads. All the targets with the same group will
            be combined into a special ":{library_name}_{group_name}" library.
            Overloads without `build_target_groups` will be put into "plain"
            group by default.

            For example, if an overload for ":operator_add" has "on_optionals"
            and "on_i32" build_target_groups, then additional build targets
            ":operator_add_on_optionals" and ":operator_add_on_i32" grouping
            the corresponding overloads will be generated.

    Returns:
        Accumulator implementation specification struct.
    """
    hdr_fullpaths = _unique([_header_fullpath(h) for h in hdrs])
    dep_fullpaths = _unique([_dep_fullpath(d) for d in deps])

    if not build_target_groups:
        build_target_groups = ("plain",)

    return struct(
        parent_args = parent_args,
        child_args = child_args,
        init_args = init_args,
        acc_class = acc_class,
        hdrs = hdr_fullpaths,
        deps = dep_fullpaths,
        build_target_groups = tuple(build_target_groups),
    )

def lift_by(lifters, overloads):
    """Applies lifters to operator overloads.

    Args:
        lifters: list of lifter functions to apply.
        overloads: list of operator ioverloads.

    Returns:
        a list containing all lifted operator overloads.
    """
    return [lifter(op) for lifter in lifters for op in overloads]

def with_lifted_by(lifters, overloads):
    """Suppliments operator overloads with their lifted versions.

    Args:
        lifters: list of lifter functions to apply.
        overloads: list of operator overloads.

    Returns:
        a list containing all the overloads and their lifted versions.
    """
    return overloads + lift_by(lifters, overloads)

def meta_type_list(args):
    return "::arolla::meta::type_list<{}>".format(", ".join(args))

def _make_optional_op(t, args):
    return ("::arolla::OptionalLiftedOperator<{}, {}>").format(
        t,
        meta_type_list(args),
    )

def lift_to_optional(op):
    """Lifts pointwise operator implementation to support OptionalValue types."""
    return operator_overload(
        args = [(make_optional_type(a) if is_liftable(a) else a) for a in op.args],
        op_class = _make_optional_op(op.op_class, op.args),
        hdrs = op.hdrs + ["arolla/qexpr/lift_to_optional_operator.h"],
        deps = op.deps + ["//arolla/qexpr"],
        build_target_groups = ["on_optionals"],
    )

def _make_scalar_group_op(acc):
    return "::arolla::ScalarToScalarGroupLifter<{},{},{}>".format(
        acc,
        acc + "::parent_types",
        acc + "::child_types",
    )

def lift_accumulator_to_scalar(acc_overload):
    """Lifts accumulator to a group_op operator on Arrays.

    Args:
        acc_overload: implementation specification of the wrapped accumulator. Must be
            constructed using accumulator_overload.

    Returns:
        implementation specification for the resulting operator.
    """
    child_args = [make_optional_type(a) for a in acc_overload.child_args]
    return operator_overload(
        args = acc_overload.parent_args + child_args + ["::arolla::ScalarToScalarEdge"] + acc_overload.init_args,
        op_class = _make_scalar_group_op(acc_overload.acc_class),
        hdrs = acc_overload.hdrs + ["arolla/qexpr/lift_accumulator_to_scalar_operator.h"],
        deps = acc_overload.deps + ["//arolla/qexpr"],
    )
