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

"""Utility to generate input loaders and slot listeners for protos."""

load(
    "//arolla/codegen:utils.bzl",
    "bash_escape",
    "call_python_function",
    "encode_call_python_function",
    "is_python_function_call",
    "make_build_script",
    "merge_dicts",
)

# core dependencies for all input loaders and slot listeners
_CORE_DEPS = [
    "@com_google_absl//absl/memory",
    "@com_google_absl//absl/types:span",
    "//arolla/io",
    "//arolla/proto",
    "//arolla/qexpr",
    "//arolla/qtype",
    "//arolla/util",
]

def _list_sum(lists):
    res = []
    for lst in lists:
        res += lst
    return res

def _join_dicts_of_lists(dicts):
    keys = {k: 1 for k in _list_sum([list(d.keys()) for d in dicts])}
    return {k: _list_sum([d.get(k, []) for d in dicts]) for k in keys}

_PROTO_NO_ARRAY_DEPS = [
    "//arolla/util",
    "//arolla/memory",
    "//arolla/proto",
    "//arolla/codegen/io:multi_loader",
]

_PROTO_NO_ARRAY_DEPS_PER_ARRAY = {key: _PROTO_NO_ARRAY_DEPS for key in ["", "DenseArray"]}

_PROTO_EXTRA_DEPS = _join_dicts_of_lists(
    [
        _PROTO_NO_ARRAY_DEPS_PER_ARRAY,
        {
            "DenseArray": [
                "//arolla/dense_array",
                "//arolla/dense_array/qtype",
            ],
        },
    ],
)

_TEXT_EXTRA_DEPS = {
    key: [
        "//arolla/util",
    ]
    for key in ["", "DenseArray"]
}

_TEXT_EXTRA_HEADERS = {
    key: [
        "arolla/util/text.h",
    ]
    for key in ["", "DenseArray"]
}

_BYTES_EXTRA_DEPS = {
    key: [
        "//arolla/util",
    ]
    for key in ["", "DenseArray"]
}

_BYTES_EXTRA_HEADERS = {
    key: [
        "arolla/util/bytes.h",
    ]
    for key in ["", "DenseArray"]
}

def _policy_info(name):
    "Returns naming policy information."
    return struct(name = name)

# LINT.IfChange
proto_naming_policy = struct(
    # have leading '/', separate nested fields with '/'
    # e.g., '/x' or '/inners/a'
    default = _policy_info(name = "default"),
    # have no leading symbol, separate nested fields with ('_' * 2)
    # e.g., 'x' or 'inners__a'
    double_underscore = _policy_info(name = "double_underscore"),
    # have no leading symbol, separate nested fields with '_'
    # e.g., 'x' or 'inners_a'
    # Name duplications are possible, do not use for big protos.
    single_underscore = _policy_info(name = "single_underscore"),
    # have no leading symbol and no separators; nested fields are identified
    # only by the leaf name.
    # e.g., 'x' or 'a'
    # Name duplications are very likely, do not use for big protos.
    leaf_only = _policy_info(name = "leaf_only"),
)
# LINT.ThenChange(
#     //depot/arolla/naming/policy.h)

def _type_info(cpp_type, array_type_deps, array_type_hdrs):
    "Returns information required for using the cpp type in code generation."
    return struct(cpp_type = cpp_type, array_type_deps = array_type_deps, array_type_hdrs = array_type_hdrs)

# types to explicitly override default in the generated code
io_type = struct(
    text = _type_info(
        cpp_type = "::arolla::Text",
        array_type_deps = _TEXT_EXTRA_DEPS,
        array_type_hdrs = _TEXT_EXTRA_HEADERS,
    ),
    bytes = _type_info(
        cpp_type = "::arolla::Bytes",
        array_type_deps = _BYTES_EXTRA_DEPS,
        array_type_hdrs = _BYTES_EXTRA_HEADERS,
    ),
)

def sharding_info(shard_count):
    """Returns struct with a sharding information."""
    return struct(shard_count = shard_count)

def proto_fields_accessor(
        py_proto_name,
        py_deps,
        proto_extension_modules = None,
        skip_field_fn = None,
        naming_policy = proto_naming_policy.default,
        text_type = io_type.text,
        protopath_prefix = None):
    """Creates accessor for multiple proto fields based on the descriptor.

    Information about proto fields is stored inside of proto descriptor.
    Can be used as mutable accessor in slot_listener.

    Args:
        py_proto_name: (str) fully qualified python proto name 'package_name.ProtoClassName'
        py_deps: (List[str])
            py_proto_library's for the corresponding proto and extensions
            py_library for skip_field_fn if not None
        proto_extension_modules: (List[str]) list of fully qualified python proto packages to
            search extensions. Accessors to all extensions and children fields defined in these
            packages will be generated
        skip_field_fn: (Optional[str]) fully qualified function name with signature:
            `def skip_fn(d : descriptor.FieldDescriptor) -> bool`. If True is returned the field
            will be skipped.
        naming_policy: (proto_naming_policy.*) one of the supported naming policy
        text_type: (io_type.text (default) or io_type.bytes)
                the type to use for string proto fields
        protopath_prefix: (str) prefix for all protopath's generated by the accessor.
            Useful for cases, when input_cls of input loader containing `py_proto_name`.
            E.g., protopath_prefix="[:]" can be useful for `absl::Span<const Proto>`
               or protopath_prefix="signals" can be useful to generate accessors for
               subtree of the protos.
    Returns:
      accessor of many OptionalValue's and/or Array's for the specified proto.
    """
    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.protopath_descriptor_accessors",
            args = [],
            kwargs = dict(
                proto_name = py_proto_name,
                extension_modules = proto_extension_modules or [],
                naming_policy = naming_policy.name,
                text_cpp_type = text_type.cpp_type,
                skip_field_fn = skip_field_fn,
                protopath_prefix = protopath_prefix,
            ),
            deps = [
                "//arolla/codegen/io",
            ] + py_deps,
        ),
        array_type_deps = _join_dicts_of_lists([text_type.array_type_deps, _PROTO_EXTRA_DEPS]),
        array_type_hdrs = text_type.array_type_hdrs,
    )

def _protopath_accessor_type(path, type):
    """Returns _type_info struct with type for the accessor.

    If type is None, `result.cpp_type` is empty string."""
    if path.startswith("/"):
        path = path[1:]
    elements = path.split("/")
    if not elements:
        fail("path is empty: " + path)
    has_array = False
    for el in elements:
        if not el:
            fail("empty element in the path: " + path)
        has_array = has_array or el.endswith("[:]")

    array_type_deps = _PROTO_EXTRA_DEPS if has_array else _PROTO_NO_ARRAY_DEPS_PER_ARRAY
    array_type_hdrs = {}
    if type != None:
        array_type_deps = _join_dicts_of_lists([array_type_deps, type.array_type_deps])
        array_type_hdrs = _join_dicts_of_lists([array_type_hdrs, type.array_type_hdrs])
    return _type_info(
        cpp_type = type.cpp_type if type else "",
        array_type_deps = array_type_deps,
        array_type_hdrs = array_type_hdrs,
    )

def protopath_accessor_deps():
    """Returns list of dependencies required for protopath accessors.

    These dependencies are automatically added when proto* accessors are used.
    This is useful for users of accessor_generator that use
    from arolla.codegen.io import protopath
    in the generator.
    """
    deps = _CORE_DEPS + _PROTO_EXTRA_DEPS[""] + _PROTO_EXTRA_DEPS["DenseArray"]
    return sorted({d: 1 for d in deps}.keys())

def protopath_accessor(path, name = None, type = None):
    """Returns accessor of OptionalValue or Array by provided XPath-like string.

    The input is expected to be proto or struct with protos inside.

    This accessor has default name implementation.
    ::arolla::Bytes element type is used for string fields by default.
    Pass type = io_type.text to use ::arolla::Text.

    Supported XPath operators:
      `/`:  Child operator: selects immediate child of the left-side message.
          Child must not be a repeated field.
      `[:]`: selects all elements from the input container (array-like type will be returned).
      `field[:]`: selects all elements from repeated field (array-like type will be returned).
           Repeated primitive fields are not supported in slot_listener.
      `count(field[:])` : counts (or resizes in slot_listener) elements in the repeated field.
           Must be the last element in path.
           If there is no other repeated fields in the path ArrayShape object will be returned.
           Otherwise Array-like type will be returned with dtype ::arolla::proto::arolla_size_t.
           Repeated primitive fields are not supported in slot_listener.
      `[i]`: selects ith element from repeated field
      `["key"]`: selects element with "key" from map<string, T> field
      `[:]/@key`: selects all sorted keys in the map (not supported in slot_listener)
      `[:]/@value`: selects all values sorted by keys in the map (not supported in slot_listener)
      `Ext::field_full_name`: selects specified extension field

    Structpath operators (for input C++ `struct`s with mix of struct/proto fields):
      `*`: dereference a pointer
      `&`: take an address
      `&::field`: access to the field in C++ struct

    Args:
      path: protopath string
      name: string name for the protopath. If None default name is used.
      type: (io_type.*) One of the supported type. If None default type is used.

    Returns:
      accessor of OptionalValue or Array for the specified proto field.

    Examples:
      message Root {
        optional int32_t x = 1;
        repeated int32_t ys = 2;
        message HyperInner {
          optional int32_t q = 1;
        }
        message Inner {
          optional int32_t a = 1;
          repeated int32_t as = 2;
          repeated HyperInner hs = 3;
        }
        optional Inner inner = 3;
        repeated Inner inners = 4;
      }
      "x" gives field `x`
          value is missing if x is missing from proto.
      "inner/a" gives field `a` inside of field `inner`
          value is missing if either `inner` or `a` is missing from proto.
      "inner/&" gives a `const Inner*` pointer to `inner`
      "ys[:]" gives full array with numbers from field `ys`
      "[:]/x" gives array of `x` fields from container of Root's (e.g., absl::Span<const Root>)
          value is missing if x is missing from proto.
      "inner/as[:]" gives full array with numbers from field `as` inside `inner`
      "inners[:]/as[:]" gives full array with numbers from field `as` inside `inners`
          numbers from all `inners` fields are concatenated
      "inners[:]/&" gives an array with `const Inner*` pointers to members of `inners`
      "inners[:]/a" gives array with numbers from field `a` inside `inners`
          item in arrays is missing if `a` missing in the `inners` field
      "count([:])" gives ArrayShape with size of the input container
           (e.g., absl::Span<const Root>::size())
      "count(inners[:])" gives ArrayShape with size of the repeated field `inners`.
           In slot_listener case resize field `inners` to the provided size.
      "inners[:]/count(as[:])" gives array of length `inners_size()`
          with sizes of the repeated field `as`.
          Not supported in slot_listener.
      "inners[:]/count(hs[:])" gives array of length `inners_size()`
          with sizes of the repeated field `hs`.
          In slot_listener case resize field `hs` in each `inners` to the provided size.
          `inners` must be resized via `count(inners[:])` (or before call to SlotListener) to the
          size of array.

    Struct examples:
      struct RootNative {
        int x = 0;
        RootNative* root_struct = nullptr;
        Root root_proto;
        Root* root_proto_ptr = nullptr;
      };

      "&::x" gives field `x`. Value is never missing.
      "&::root_struct/*/&::x" gives field `x` inside of root_struct.
          Value is missing if root_struct is nullptr.
      "&::root_proto/x" gives field `x` inside of root_proto.
          Value is missing if x is missing in root_proto.
      "&::root_proto_ptr/*/x" gives field `x` inside of root_proto_ptr.
          Value is missing if root_proto_ptr is nullptr or x is missing in root_proto_ptr.
    """
    type = _protopath_accessor_type(path, type)

    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.protopath_accessor",
            args = [path, name or "", type.cpp_type],
            deps = [
                "//arolla/codegen/io",
            ],
        ),
        array_type_deps = type.array_type_deps,
        array_type_hdrs = type.array_type_hdrs,
    )

def accessor_generator(generator_fn, deps = None, array_type_deps = None, array_type_hdrs = None):
    """Creates accessor for multiple fields based on the generation function.

    Args:
        generator_fn: (call_python_function) returning
            Callable[[accessor_generator.Config], list[tuple[str, accessors.Accessor]]].
          The first element in the returned tuple is name, if name is empty default_name is used.
        deps: (list[str]) list of dependencies for generated accessors
        array_type_deps: (dict[str, list[str]]) additional dependencies per array type ("" or "DenseArray")
        array_type_hdrs: (dict[str, list[str]]) additional headers to include per array type ("" or "DenseArray")
    Returns:
      accessor of many OptionalValue's and/or Array's generated by generator_fn.
    """
    if not is_python_function_call(generator_fn):
        fail("model must be `call_python_function`")
    return struct(
        generator_fn = generator_fn,
        deps = list(deps or []) + _PROTO_NO_ARRAY_DEPS,
        array_type_deps = array_type_deps or {},
        array_type_hdrs = array_type_hdrs or {},
    )

def merge_accessor_list(accessor_list):
    """Returns accessor_generator combining all accessors from the list.

    This function is useful in case further transformation (like filtering) is required for the list.

    Args:
      accessor_list: list of other accessors

    Returns:
      accessor_generator providing all accessors together.
    """
    if len(accessor_list) == 1:
        # minor optimization to avoid unnecessary nesting
        return accessor_list[0]
    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.merge_accessor_list",
            args = [accessor.generator_fn for accessor in accessor_list],
            deps = [
                "//arolla/codegen/io",
            ],
        ),
        deps = _list_sum([accessor.deps for accessor in accessor_list]),
        array_type_deps = _join_dicts_of_lists([accessor.array_type_deps for accessor in accessor_list]),
        array_type_hdrs = _join_dicts_of_lists([accessor.array_type_hdrs for accessor in accessor_list]),
    )

def allowed_from_set_filter(names):
    """Returns call_python_function that returns filter for specific names.

    Args:
      names: list[str] or call_python_function returning list[str]
    """
    return call_python_function(
        "arolla.codegen.io.accessor_generator.allowed_from_set",
        args = [names],
        deps = [
            "//arolla/codegen/io",
        ],
    )

def filtered_by_name_accessor(accessor_list, is_allowed):
    """Returns accessor_generator that keep only accessors with is_allowed(name).

    Args:
      accessor_list: list of accessors
      is_allowed: call_python_function returning Callable[[str], bool].
          It can be useful to use predefined filter, e.g., allowed_from_set_filter.

    Returns:
      accessor_generator providing allowed accessors.
    """
    merged_accessor = merge_accessor_list(accessor_list)
    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.filtered_by_name_accessor",
            args = [merged_accessor.generator_fn, is_allowed],
            deps = [
                "//arolla/codegen/io",
            ],
        ),
        deps = merged_accessor.deps,
        array_type_deps = merged_accessor.array_type_deps,
        array_type_hdrs = merged_accessor.array_type_hdrs,
    )

def filtered_for_models_accessor(accessor_list, model_io_infos):
    """Returns accessor_generator that keep only accessors for given models.

    For input_loader filtering happen by inputs, for slot_listener
    filtering happen by side_outputs.

    Args:
      accessor_list: list of accessors
      model_io_infos: list of targets with textproto with ModelInputOutputInfo.

    Returns:
      accessor_generator providing accessors required for the models.
    """
    merged_accessor = merge_accessor_list(accessor_list)
    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.filtered_for_models_accessor",
            args = [merged_accessor.generator_fn, ["$(location %s)" % f for f in model_io_infos]],
            deps = [
                "//arolla/codegen/io",
            ],
            data = model_io_infos,
        ),
        deps = merged_accessor.deps,
        array_type_deps = merged_accessor.array_type_deps,
        array_type_hdrs = merged_accessor.array_type_hdrs,
    )

def wildcard_protopath_accessor(path, name = None, type = None):
    """Returns wildcard accessor by provided XPath-like string.

    Should be used only with wildcard_input_loaders.
    See `protopath_accessor` documentation for detailed explanation of supported
    operators.

    Protopath have to contain exactly one wildcard lookup to the map field in the
    following form:
      [*]: selects element with wildcard key from map<string, T> field

    Args:
      path: protopath string
      name: string name pattern for the protopath with a single "%s".
         E.g. int_wildcard_%s. If None default name is used.
      type: (io_type.*) One of the supported type. If None default type is used.

    Returns:
      accessor of OptionalValue or Array for the specified proto field.

    Examples:
      message Root {
        message Inner {
          optional int32_t a = 1;
          repeated int32_t as = 2;
          map<string, float> map_float = 7;
        }
        repeated Inner inners = 4;
        map<string, int32> map_int = 6;
        map<string, Inner> map_str_inner = 14;
      }
      "map_int[*]" gives value inside of `map_int` accessed by wildcard key.
          value is missed if key is not present in the map.
      "map_str_inner[*]/a" gives `a` field inside of `map_str_inner` accessed by wildcard key.
          Value is missed if key is not present in the map or `a` is missed.
      "map_str_inner[*]/as[:]" gives full array with numbers from field `as`
          inside of the value in `map_str_inner` accessed by wildcard key.
          Array has length 0 if key is not present in the map
      "inners[:]/map_float[*]" gives full array of length `inners_size()`
          with values from field `map_float` accessed by wildcard key.
          Array contains missed values whenever if key is not present in the map.
    """
    type = _protopath_accessor_type(path, type)

    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.wildcard_protopath_accessor",
            args = [path, name or "", type.cpp_type],
            deps = ["//arolla/codegen/io"],
        ),
        array_type_deps = _join_dicts_of_lists([type.array_type_deps, _PROTO_EXTRA_DEPS]),
        array_type_hdrs = type.array_type_hdrs,
    )

def path_accessor(path, name):
    """Returns accessor by appending path to the input.

    Args:
      path: string to append after parameter name to get the field.
      name: string name for the accessor.

    Examples:
      struct InputType {
        int x;
        std::vector<std::pair<int, int>> v;
      };

      ".x" gives field `x`
      ".v.size()" gives length of the field v
      ".v.back().first" gives the first element of the last pair in field v
      ".x & 1" gives the last bit of the field x
    """
    return accessor_generator(
        call_python_function(
            "arolla.codegen.io.accessor_generator.path_accessor",
            args = [path, name],
            deps = ["//arolla/codegen/io"],
        ),
    )

def _collect_deps_info_from_accessors_and_headers(accessors, array_type, deps, hdrs):
    """Returns struct with deps, tool_deps and hdrs."""
    tool_deps = []
    tool_data = []

    deps = list(deps)
    hdrs = list(hdrs)
    deps += _CORE_DEPS
    for accessor in accessors:
        deps += accessor.array_type_deps.get(array_type, []) + accessor.deps
        tool_deps += accessor.generator_fn.deps
        tool_data += accessor.generator_fn.data
        hdrs += accessor.array_type_hdrs.get(array_type, [])

    for hdr in hdrs:
        if is_python_function_call(hdr):
            tool_deps += hdr.deps
            tool_data += hdr.data

    deps = sorted({d: 1 for d in deps}.keys())
    tool_deps = sorted({d: 1 for d in tool_deps}.keys())
    tool_data = sorted({d: 1 for d in tool_data}.keys())
    hdrs = sorted({h: 1 for h in hdrs}.keys())
    return struct(
        deps = deps,
        tool_deps = tool_deps,
        tool_data = tool_data,
        hdrs = hdrs,
    )

def _collect_build_info(name):
    """Returns struct with build information."""
    package_name = native.package_name()
    return struct(
        target = "//{}:{}".format(package_name, name),
        output_dir = "$(GENDIR)/{}".format(package_name),
        h_file = name + ".h",
        cc_file = name + ".cc",
        gen_rule = name + "_gen_cc_and_h",
    )

def _shard_cc_files(name, shard_count):
    return [name + "_" + str(i) + ".cc" for i in range(1, shard_count)]

def _build_info_flags(build_info):
    """Returns flags with build information."""
    return ("--output_dir={output_dir} --build_target={build_target} " +
            "--h_out_file={h_out_file} --cc_out_file={cc_out_file}").format(
        output_dir = build_info.output_dir,
        build_target = build_info.target,
        h_out_file = build_info.h_file,
        cc_out_file = build_info.cc_file,
    )

def input_loader(
        name,
        loader_name,
        input_cls,
        accessors,
        hdrs,
        deps = [],
        sharding = sharding_info(0),
        array_type = "DenseArray",
        tool_deps = [],
        tool_data = [],
        **kwargs):
    """Input loader C++ library with provided name and header file `{name}.h`

    Types of the inputs will be determinated automatically. More details can be
    found in AccessorsInputLoader documentation.

    Library contains
     * zero argument function with name `loader_name`
       returning `const ::arolla::InputLoader<{input_cls}>&`.
    * (only if `sharding` is used, see details below) function `{loader_name}_Shards`.

    Args:
      name: name of the target and basename of the header and cc file
      loader_name: fully qualified name of the loader
          C++ function with this name will be generated under
          namespace taken from the name. E.g. ::contrib::x::GetMyLoader.
      input_cls: fully qualified C++ typename of the input, or
        call_python_function spec that generates it.
      accessors: list of accessors.
          Accessor can be constructed by one of the *_accessor function in this bzl.
          For custom accessor creation use accessor_generator bzl function.
          Anything that extracting data from the input value called accessor.
          Accessed data could be virtual, e.g., size of the array or floats fractional part.
      hdrs: list of header files need to be included.
          Typically contains at least header for the input class.
          Standard headers must include <>, e.g., <tuple>
          May contain call_python_function spec that generates list of headers.
      deps: list of required dependencies
          Typically contains at least dependency for the input class
      sharding: configuration for sharding resulting InputLoader.
          In case `sharding.shard_count` is not 0, additional function
          `{loader_name}_Shards` returning `std::vector<::arolla::InputLoaderPtr<T>>`
          will be created. The main function will return `ChainInputLoader` merging all shards.
          Sharding is useful for compilation optimization for enormously big loaders,
          customization of `ChainInputLoader` (e.g., for multithreading), or
          to speed up case with many unused fields (require benchmarking).

      array_type: "DenseArray" or "" the type of vector to use
        for accessors returning arrays:
          "DenseArray" signal to use `::arolla::DenseArray<T>`
          "" signal that array_types are forbidden
      tool_deps: additional deps for the codegen tool.
      tool_data: additional data deps for the codegen tool.
      **kwargs: other arguments required for the native.cc_library
    """
    if array_type not in ["DenseArray", ""]:
        fail("Unknown array type", array_type)
    testonly = kwargs.get("testonly", 0)
    tool_deps = list(tool_deps)
    tool_data = list(tool_data)

    hdr_getters = [
        encode_call_python_function(h)
        for h in hdrs
        if is_python_function_call(h)
    ]
    hdrs = [h for h in hdrs if not is_python_function_call(h)]

    merged_accessor = merge_accessor_list(accessors)
    dep_info = _collect_deps_info_from_accessors_and_headers(
        [merged_accessor],
        array_type,
        deps + ["//arolla/io", "@com_google_absl//absl/types:span"],
        hdrs,
    )
    build_info = _collect_build_info(name)

    accessor_flags = " ".join([
        '--accessor_generator="%s"' % encode_call_python_function(
            accessor.generator_fn,
        )
        for accessor in accessors
    ])

    if is_python_function_call(input_cls):
        input_cls_flag = '--input_cls_getter="{}"'.format(
            encode_call_python_function(input_cls),
        )
        tool_deps += input_cls.deps
        tool_data += input_cls.data
    else:
        input_cls_flag = '--input_cls="{}"'.format(input_cls)

    loader_binary = make_build_script(
        name = name + "_input_loader",
        script = "//arolla/codegen/io:input_loader_main.py",
        testonly = testonly,
        deps = depset([
            "//arolla/codegen/io:input_loader_main",
        ] + dep_info.tool_deps + tool_deps).to_list(),
        data = depset(dep_info.tool_data + tool_data).to_list(),
    )

    shard_cc_files = _shard_cc_files(name, sharding.shard_count)

    native.genrule(
        name = build_info.gen_rule,
        outs = [build_info.h_file, build_info.cc_file] + shard_cc_files,
        cmd = ("$(execpath :" + loader_binary + ") " +
               '--array_type="{array_type}" {build_info_flags} ' +
               "--loader_name={loader_name} --sharding_info={sharding_info} {input_cls_flag} " +
               "{hdrs} {hdr_getters} {accessor_flags}").format(
            array_type = array_type,
            build_info_flags = _build_info_flags(build_info),
            input_cls_flag = input_cls_flag,
            loader_name = loader_name,
            sharding_info = bash_escape(json.encode(sharding)),
            hdrs = " ".join(["--hdrs=%s" % h for h in dep_info.hdrs]),
            hdr_getters = " ".join(["--hdr_getters=\"%s\"" % h for h in hdr_getters]),
            accessor_flags = accessor_flags,
        ),
        testonly = testonly,
        tools = [loader_binary] + dep_info.tool_data,
    )

    native.cc_library(
        name = name,
        srcs = [build_info.cc_file] + shard_cc_files,
        hdrs = [build_info.h_file],
        deps = dep_info.deps,
        **kwargs
    )

def _collect_deps_info_from_specs(specs):
    if is_python_function_call(specs):
        return struct(
            deps = [],
            tool_deps = specs.deps,
            tool_data = specs.data,
        )
    deps = []
    tool_deps = []
    tool_data = []
    for _, spec in specs.items():
        for _, arg in spec.items():
            if is_python_function_call(arg):
                tool_deps += arg.deps
                tool_data += arg.data
            elif type(arg) == type([]):
                for v in arg:
                    if is_python_function_call(v):
                        tool_deps += v.deps
                        tool_data += v.data
                    elif hasattr(v, "generator_fn"):
                        array_type = spec.get("array_type", "DenseArray")
                        deps += v.array_type_deps.get(array_type, []) + v.deps
                        tool_deps += v.generator_fn.deps
                        tool_data += v.generator_fn.data
    return struct(
        deps = deps,
        tool_deps = tool_deps,
        tool_data = tool_data,
    )

def input_loader_set(
        name,
        loaders_spec,
        deps = [],
        tool_deps = [],
        tool_data = [],
        max_shard_count = 0,
        **kwargs):
    """C++ library with a set of input loaders and header file `{name}.h`

    Types of the inputs will be determinated automatically. More details can be
    found in AccessorsInputLoader documentation.

    Library contains a set of functions returning input loaders. For each input loader there are
    several functions (`{loader_name}`, `{loader_name}_Shards`). See the description of the
    `input_loader` build rule for details.

    Args:
      name: name of the target and basename of the header and cc file.
      loaders_spec: a dict[LoaderName, Spec], a call_python_function that generates the dict, or
          a list of such call_python_functions. LoaderName is a fully qualified name of an input
          loader. C++ function with this name will be generated under namespace taken from the name.
          E.g. "::contrib::x::GetMyLoader".
          Spec is a dict of the loader arguments with the keys:
          - input_cls: fully qualified C++ typename of the input.
          - accessors: list of accessors or accessor generators.
              Anything that extracting data from the input value called accessor.
              Accessed data could be virtual, e.g., size of the array or floats fractional part.
          - hdrs: list of header files need to be included.
              Typically contains at least header for the input class.
              Standard headers must include <>, e.g., <tuple>.
          - sharding: configuration for sharding resulting input loaders.
              In case `sharding.shard_count` is not 0, for each input loaded an additional function
              `{loader_name}_Shards` returning `std::vector<::arolla::InputLoaderPtr<T>>`
              will be created. The main function will return `ChainInputLoader` merging all shards.
              Sharding is useful for compilation optimization for enormously big loaders,
              customization of `ChainInputLoader` (e.g., for multithreading), or
              to speed up case with many unused fields (require benchmarking).
          - array_type: "DenseArray" or "" the type of vector to use for accessors returning arrays:
              "DenseArray" signal to use `::arolla::DenseArray<T>`
              "" signal that array_types are forbidden
      deps: list of required dependencies
          Typically contains at least dependency for the input classes.
      tool_deps: additional deps for the codegen tool.
      tool_data: additional data deps for the codegen tool.
      max_shard_count: maximum number of `sharding.shard_count` across the loaders_spec.
          This number is required in the build time in order to configure genrule output files.
      **kwargs: other arguments required for the native.cc_library
    """
    testonly = kwargs.get("testonly", 0)
    if type(loaders_spec) == type([]):
        loaders_spec = merge_dicts(*loaders_spec)
    deps_info = _collect_deps_info_from_specs(loaders_spec)
    deps = depset(deps + deps_info.deps + _CORE_DEPS).to_list()
    tool_deps = depset(tool_deps + deps_info.tool_deps).to_list()
    tool_data = depset(tool_data + deps_info.tool_data).to_list()

    loaders_binary = make_build_script(
        name = name + "_build_script",
        script = "//arolla/codegen/io:input_loader_set_main.py",
        testonly = testonly,
        deps = depset([
            "//arolla/codegen/io:input_loader_set_main",
        ] + tool_deps).to_list(),
        data = tool_data,
    )

    shard_cc_files = _shard_cc_files(name, max_shard_count)

    build_info = _collect_build_info(name)
    native.genrule(
        name = build_info.gen_rule,
        outs = [build_info.h_file, build_info.cc_file] + shard_cc_files,
        cmd = ("$(execpath :" + loaders_binary + ") " +
               '{build_info_flags} --loaders_spec="{loaders}" ' +
               "--max_shard_count={max_shard_count}").format(
            build_info_flags = _build_info_flags(build_info),
            loaders = encode_call_python_function(loaders_spec),
            max_shard_count = max_shard_count,
        ),
        testonly = testonly,
        tools = [loaders_binary] + tool_data,
    )

    native.cc_library(
        name = name,
        srcs = [build_info.cc_file] + shard_cc_files,
        hdrs = [build_info.h_file],
        deps = deps,
        **kwargs
    )

def wildcard_input_loaders(
        name,
        input_cls,
        loader_name2accessor,
        hdrs,
        deps = [],
        array_type = "DenseArray",
        **kwargs):
    """Input loader C++ library with provided name and header file `{name}.h`

    Types of the inputs will be determinated automatically. More details can be
    found in AccessorsInputLoader documentation.

    Library contains `len(loader_name2accessor)` unary functions with
    names corresponding to `loader_name2accessor.keys()`.
    Function signature is:
    absl::StatusOr<::arolla::InputLoaderPtr<{input_cls}>>
    {loader_name}(absl::Span<const std::string> keys);

    Args:
      name: name of the target and basename of the header and cc file
      input_cls: fully qualified typename of the input
      loader_name2accessor: dict from fully qualified name of the loader
          to the wildcard accessor.
          C++ functions will be generated for every key in the dict under
          namespace and with a name taken from the key.
          E.g. {"::contrib::x::GetMyLoader",
                wildcard_protopath_accessor("map_int[*]")}.
          accessor is only constructible by one of the wildcard_*_accessor functions
          in this bzl.
          Anything that extracting data from the input value called accessor.
          Accessed data could be virtual, e.g., size of the array or floats fractional part.
      hdrs: list of header files need to be included.
          Typically contains at least header for the input class.
          Standard headers must include <>, e.g., <tuple>
          May contain call_python_function spec that generates list of headers.
      deps: list of required dependencies
          Typically contains at least dependency for the input class
      array_type: "DenseArray" or "" the type of vector to use
        for accessors returning arrays:
          "DenseArray" signal to use `::arolla::DenseArray<T>`
          "" signal that array_types are forbidden
      **kwargs: other arguments required for the native.cc_library
    """
    if array_type not in ["DenseArray", ""]:
        fail("Unknown array type", array_type)
    testonly = kwargs.get("testonly", 0)

    dep_info = _collect_deps_info_from_accessors_and_headers(
        loader_name2accessor.values(),
        array_type,
        deps + [
            "//arolla/io",
            "@com_google_absl//absl/types:span",
        ],
        hdrs,
    )
    build_info = _collect_build_info(name)

    accessor_flags = " ".join(
        [
            '--accessor_generator="%s" --loader_name="%s"' % (
                encode_call_python_function(accessor.generator_fn),
                loader_name,
            )
            for loader_name, accessor in sorted(loader_name2accessor.items())
        ],
    )

    loader_binary = make_build_script(
        name = name + "_wildcard_input_loaders",
        script = "//arolla/codegen/io:wildcard_input_loaders_main.py",
        testonly = testonly,
        deps = [
            "//arolla/codegen/io:wildcard_input_loaders_main",
        ] + dep_info.tool_deps,
    )
    native.genrule(
        name = build_info.gen_rule,
        outs = [build_info.h_file, build_info.cc_file],
        cmd = ("$(execpath :" + loader_binary + ") " +
               '--array_type="{array_type}" {build_info_flags} ' +
               '--input_cls="{input_cls}" ' +
               "{hdrs} {accessor_flags}").format(
            output_dir = build_info.output_dir,
            array_type = array_type,
            build_info_flags = _build_info_flags(build_info),
            input_cls = input_cls,
            hdrs = " ".join(["--hdrs=%s" % h for h in dep_info.hdrs]),
            accessor_flags = accessor_flags,
        ),
        testonly = testonly,
        tools = [loader_binary],
    )

    native.cc_library(
        name = name,
        srcs = [build_info.cc_file],
        hdrs = [build_info.h_file],
        deps = dep_info.deps,
        **kwargs
    )

def slot_listener(
        name,
        slot_listener_name,
        output_cls,
        accessors,
        hdrs,
        deps = [],
        array_type = "DenseArray",
        tool_deps = [],
        tool_data = [],
        **kwargs):
    """SlotListener C++ library with provided name and header file `{name}.h`

    Library contains exactly one no arguments function with name `slot_listener_name`
    returning `const ::arolla::SlotListener<output_cls>&`.

    Args:
      name: name of the target and basename of the header and cc file
      slot_listener_name: fully qualified name of the slot listener
          C++ function with this name will be generated under
          namespace taken from the name. E.g. ::contrib::x::GetMyListener.
      output_cls: fully qualified C++ typename of the output, or
        call_python_function spec that generates it. Don't forget to add
        tool_deps if the latter is used.
      accessors: list of accessors.
          Accessor can be constructed by one of the *_accessor function in this bzl.
          For custom accessor creation use accessor_generator bzl function.
          Only accessors supporting mutable access are allowed.
      hdrs: list of header files need to be included.
          Typically contains at least header for the output class.
          Standard headers must include <>, e.g., <tuple>
      deps: list of required dependencies
          Typically contains at least dependency for the input class
      array_type: "DenseArray" or "" the type of vector to use
        for accessors returning arrays:
          "DenseArray" signal to use `::arolla::DenseArray<T>`.
          "" signal that array_types are forbidden
      tool_deps: additional deps for the codegen tool.
      tool_data: additional data deps for the codegen tool.
      **kwargs: other arguments required for the native.cc_library
    """
    if array_type not in ["DenseArray", ""]:
        fail("Unknown array type", array_type)
    testonly = kwargs.get("testonly", 0)
    tool_deps = list(tool_deps)
    tool_data = list(tool_data)

    hdr_getters = [
        encode_call_python_function(h)
        for h in hdrs
        if is_python_function_call(h)
    ]
    hdrs = [h for h in hdrs if not is_python_function_call(h)]

    dep_info = _collect_deps_info_from_accessors_and_headers(
        accessors,
        array_type,
        deps + [
            "//arolla/io",
            "//arolla/memory",
            "//arolla/proto",
            "//arolla/util",
            "@com_google_absl//absl/strings",
        ],
        hdrs,
    )
    build_info = _collect_build_info(name)

    if is_python_function_call(output_cls):
        output_cls_flag = '--output_cls_getter="{}"'.format(
            encode_call_python_function(output_cls),
        )
        tool_deps += output_cls.deps
        tool_data += output_cls.data
    else:
        output_cls_flag = '--output_cls="{}"'.format(output_cls)

    accessor_flags = " ".join([
        '--accessor_generator="%s"' % encode_call_python_function(
            accessor.generator_fn,
        )
        for accessor in accessors
    ])

    listener_binary = make_build_script(
        name = name + "_slot_listener",
        script = "//arolla/codegen/io:slot_listener_main.py",
        testonly = testonly,
        deps = depset([
            "//arolla/codegen/io:slot_listener_main",
        ] + dep_info.tool_deps + tool_deps).to_list(),
        data = depset(dep_info.tool_data + tool_data).to_list(),
    )
    native.genrule(
        name = build_info.gen_rule,
        outs = [build_info.h_file, build_info.cc_file],
        cmd = ("$(execpath :" + listener_binary + ") " +
               '--array_type="{array_type}" {build_info_flags} ' +
               "--slot_listener_name={slot_listener_name} " +
               "{output_cls_flag} {hdrs} {hdr_getters} {accessor_flags}").format(
            array_type = array_type,
            build_info_flags = _build_info_flags(build_info),
            output_cls_flag = output_cls_flag,
            slot_listener_name = slot_listener_name,
            hdrs = " ".join(["--hdrs=%s" % h for h in dep_info.hdrs]),
            hdr_getters = " ".join(["--hdr_getters=\"%s\"" % h for h in hdr_getters]),
            accessor_flags = accessor_flags,
        ),
        testonly = testonly,
        tools = [listener_binary] + dep_info.tool_data,
    )

    native.cc_library(
        name = name,
        srcs = [build_info.cc_file],
        hdrs = [build_info.h_file],
        deps = dep_info.deps,
        **kwargs
    )

def slot_listener_set(
        name,
        listeners_spec,
        deps = [],
        tool_deps = [],
        tool_data = [],
        **kwargs):
    """C++ library with a set of slot listeners and header file `{name}.h`

    Library contains a set of zero argument functions
    returning `const ::arolla::SlotListener<{output_cls}>&`.

    Args:
      name: name of the target and basename of the header and cc file.
      listeners_spec: a dict[ListenerName, Spec] or a call_python_function that generates the dict,
          or a list of such call_python_functions. ListenerName is a fully qualified name of a slot
          listener. C++ function with this name will be generated under namespace taken from the
          name. E.g. ::contrib::x::GetMySlotListener.
          Spec is a dict of the listener arguments with the keys:
          - output_cls: fully qualified C++ typename of the input.
          - accessors: list of accessors or accessor generators.
              Only accessors supporting mutable access are allowed.
          - hdrs: list of header files need to be included.
              Typically contains at least header for the output class.
              Standard headers must include <>, e.g., <tuple>.
          - array_type: "DenseArray" or "" the type of vector to use
              for accessors returning arrays:
              "DenseArray" signal to use `::arolla::DenseArray<T>`.
              "" signal that array_types are forbidden
      deps: list of required dependencies
          Typically contains at least dependency for the output classes.
      tool_deps: additional deps for the codegen tool.
      tool_data: additional data deps for the codegen tool.
      **kwargs: other arguments required for the native.cc_library
    """
    testonly = kwargs.get("testonly", 0)
    if type(listeners_spec) == type([]):
        listeners_spec = merge_dicts(*listeners_spec)
    deps_info = _collect_deps_info_from_specs(listeners_spec)
    deps = depset(deps + deps_info.deps + _CORE_DEPS + [
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto",
        "//arolla/util",
        "@com_google_absl//absl/strings",
    ]).to_list()
    tool_deps = depset(tool_deps + deps_info.tool_deps).to_list()
    tool_data = depset(tool_data + deps_info.tool_data).to_list()

    listeners_binary = make_build_script(
        name = name + "_build_script",
        script = "//arolla/codegen/io:slot_listener_main.py",
        testonly = testonly,
        deps = depset([
            "//arolla/codegen/io:slot_listener_main",
        ] + tool_deps).to_list(),
        data = tool_data,
    )

    build_info = _collect_build_info(name)
    native.genrule(
        name = build_info.gen_rule,
        outs = [build_info.h_file, build_info.cc_file],
        cmd = ("$(execpath :" + listeners_binary + ") " +
               '{build_info_flags} --listeners_spec="{listeners}"').format(
            build_info_flags = _build_info_flags(build_info),
            listeners = encode_call_python_function(listeners_spec),
        ),
        testonly = testonly,
        tools = [listeners_binary] + tool_data,
    )

    native.cc_library(
        name = name,
        srcs = [build_info.cc_file],
        hdrs = [build_info.h_file],
        deps = deps,
        **kwargs
    )
