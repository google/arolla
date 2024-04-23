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

# Tests and testing utilities for codegen facilities.

load("@rules_python//python:defs.bzl", "py_library")
load("//arolla/codegen:utils.bzl", "call_python_function")
load(
    "//arolla/codegen/io:io.bzl",
    "accessor_generator",
    "allowed_from_set_filter",
    "filtered_by_name_accessor",
    "filtered_for_models_accessor",
    "input_loader",
    "input_loader_set",
    "io_type",
    "path_accessor",
    "proto_fields_accessor",
    "proto_naming_policy",
    "protopath_accessor",
    "sharding_info",
    "slot_listener",
    "slot_listener_set",
    "wildcard_input_loaders",
    "wildcard_protopath_accessor",
)
load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

licenses(["notice"])

py_library(
    name = "test_util",
    testonly = 1,
    srcs = ["test_util.py"],
)

cc_library(
    name = "test_native_struct",
    testonly = 1,
    hdrs = ["test_native_struct.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = ["//arolla/proto:test_cc_proto"],
)

cc_library(
    name = "test_proto_qtype",
    testonly = True,
    srcs = ["test_proto_qtype.cc"],
    hdrs = ["test_proto_qtype.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/dense_array/qtype",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
    ],
)

input_loader(
    name = "test_input_loader_compilation_stress",
    testonly = 1,
    hdrs = [],
    accessors = [
        path_accessor(
            " + %d" % i,
            name = "a%d" % i,
        )
        # as of 21.08.2020 building with -c opt succeed
        # *  with 2500 in 20 seconds
        # *  with 4500 in 70 seconds
        # *  with 6500 in 4 minutes
        # and fail with 10000 (timeout 15 minutes)
        # 1200 is set to limit load on presubmit service (wall time 15 seconds)
        for i in range(1200)
    ],
    input_cls = "int",
    loader_name = "::my_namespace_stress::GetStressLoader",
)

input_loader(
    name = "test_input_loader_compilation_stress_sharded",
    testonly = 1,
    hdrs = [],
    accessors = [
        path_accessor(
            " + %d" % i,
            name = "a%d" % i,
        )
        for i in range(10000)
    ],
    input_cls = "int",
    loader_name = "::my_namespace_stress::GetStressShardedLoader",
    sharding = sharding_info(shard_count = 100),
)

test_accessors = [
    path_accessor("", "self"),
    path_accessor(" & 1", "bit[\"0\"]"),
    path_accessor(" * 1.0", "double"),
]

input_loader(
    name = "test_input_loader",
    testonly = 1,
    hdrs = [],
    accessors = test_accessors,
    input_cls = "int",
    loader_name = "::my_namespace::GetLoader",
)

input_loader(
    name = "test_input_loader_over_sharded",
    testonly = 1,
    hdrs = [],
    accessors = test_accessors,
    input_cls = "int",
    loader_name = "::my_namespace::GetOverShardedLoader",
    sharding = sharding_info(shard_count = 100),
)

input_loader(
    name = "test_input_loader_filtered_by_fn",
    testonly = 1,
    hdrs = [],
    accessors = [filtered_by_name_accessor(
        test_accessors,
        allowed_from_set_filter(
            [
                "self",
                "double",
            ],
        ),
    )],
    input_cls = "int",
    loader_name = "::my_namespace::GetLoaderFilteredByFn",
)

input_loader(
    name = "test_input_loader_filtered_for_model",
    testonly = 1,
    hdrs = [],
    accessors = [filtered_for_models_accessor(
        [
            path_accessor("", "self"),
            path_accessor(" & 1", "bit0"),
            path_accessor(" * 1.0", "double"),
            path_accessor(" + 1", "plus1"),
        ],
        [
            "self_bit0.io.textproto",
            "bit0_plus1.io.textproto",
        ],
    )],
    input_cls = "int",
    loader_name = "::my_namespace::GetLoader",
)

cc_test(
    name = "test_input_loader_test",
    srcs = ["test_input_loader_test.cc"],
    deps = [
        ":test_generator_based_input_loader",
        ":test_input_loader",
        ":test_input_loader_over_sharded",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

input_loader(
    name = "test_descriptor_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Root",
            py_deps = ["//arolla/proto:test_py_proto"],
        ),
        proto_fields_accessor(
            # same proto with different names for fields.
            "arolla.proto.test_pb2.Root",
            naming_policy = proto_naming_policy.double_underscore,
            py_deps = [
                "//arolla/proto:test_py_proto",
            ],
            # and Bytes as a text type for str fields
            text_type = io_type.bytes,
        ),
    ],
    array_type = "DenseArray",
    input_cls = call_python_function(
        "arolla.codegen.io.testing.test_util.make_cpp_name",
        [
            "testing_namespace",
            "Root",
        ],
        deps = [":test_util"],
    ),
    loader_name = "::my_namespace::GetDescriptorBasedLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "test_descriptor_subset_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_proto3_pb2.Proto3",
            protopath_prefix = "proto3",  # path from Root to Proto3
            py_deps = ["//arolla/proto:test_proto3_py_proto"],
        ),
    ],
    array_type = "",
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetDescriptorBasedSubsetLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "test_descriptor_span_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Root",
            protopath_prefix = "[:]",  # span of input values
            py_deps = ["//arolla/proto:test_py_proto"],
        ),
    ],
    array_type = "DenseArray",
    input_cls = "absl::Span<const ::testing_namespace::Root>",
    loader_name = "::my_namespace::GetDescriptorBasedSpanLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "test_descriptor_input_loader_with_no_repeated",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Root",
            py_deps = [
                "//arolla/proto:test_py_proto",
                "//arolla/codegen/io",
            ],
            skip_field_fn = "arolla.codegen.io.protopath.is_repeated_field",
        ),
    ],
    array_type = "DenseArray",
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetDescriptorBasedLoaderWithoutRepeated",
    sharding = sharding_info(shard_count = 4),
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "test_descriptor_with_extensions_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_extension.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Root",
            proto_extension_modules = [
                "arolla.proto.test_extension_pb2",
            ],
            py_deps = [
                "//arolla/proto:test_py_proto",
                "//arolla/proto:test_extension_py_proto",
            ],
        ),
    ],
    array_type = "DenseArray",
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetDescriptorWithExtensionLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "test_dense_array_extensions_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_extension.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        # special accessors, where intermediate node
        # inners[:]/root_reference/inner/Ext::testing_extension_namespace.InnerExtension.inner_ext
        # is not always present, but this require checking parent node
        # inners[:]/root_reference/inner,
        # because we do not check existence of extensions and use
        # default instance instead (for performance reasons).
        protopath_accessor(
            "/inners[:]/root_reference/inner/Ext::testing_extension_namespace.InnerExtension.inner_ext/repeated_inner_extension_x_int32[:]",
            "inners_rr_inner_ext_xs",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/inner/Ext::testing_extension_namespace.InnerExtension.inner_ext/root_reference/repeated_floats[:]",
            "inners_rr_inner_ext_rr_floats",
        ),
    ],
    array_type = "DenseArray",
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetDenseArrayExtensionLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

input_loader(
    name = "test_descriptor_input_loader_nested_message_type",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Inner.Inner2",
            py_deps = [
                "//arolla/proto:test_py_proto",
                "//arolla/codegen/io",
            ],
        ),
    ],
    array_type = "DenseArray",
    input_cls = "::testing_namespace::Inner::Inner2",
    loader_name = "::my_namespace::GetDescriptorBasedLoaderNestedMessageType",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

cc_test(
    name = "test_descriptor_input_loader_test",
    srcs = ["test_descriptor_input_loader_test.cc"],
    deps = [
        ":test_dense_array_extensions_input_loader",
        ":test_descriptor_input_loader",
        ":test_descriptor_input_loader_nested_message_type",
        ":test_descriptor_input_loader_with_no_repeated",
        ":test_descriptor_span_input_loader",
        ":test_descriptor_subset_input_loader",
        ":test_descriptor_with_extensions_input_loader",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

input_loader(
    name = "test_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/codegen/io/testing/test_proto_qtype.h",
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor("x"),
        protopath_accessor(
            "str",
            "str[\"_\"]",
            type = io_type.text,
        ),
        protopath_accessor(
            "raw_bytes",
            "raw_bytes",
        ),
        protopath_accessor(
            "map_int['a']",
            "map_int_a",
        ),
        protopath_accessor(
            "inner/a",
            "inner__a",
        ),
        protopath_accessor(
            "ys[0]",
            "ys_0",
        ),
        protopath_accessor(
            "inners[0]/a",
            "inners_0__a",
        ),
        protopath_accessor("BrOkEn_CaSe"),
        protopath_accessor("private"),
        protopath_accessor("x_enum", "x_enum"),
        protopath_accessor("inners[0]/&"),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetProtoLoader",
    deps = [
        ":test_proto_qtype",
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "test_proto_input_loader_with_duplicated_configuration",
    testonly = 1,
    hdrs = ["arolla/proto/test.pb.h"],
    accessors = [
        protopath_accessor("x"),
        protopath_accessor("x", "y"),
        protopath_accessor("x"),
        protopath_accessor("x"),
        protopath_accessor("x", "y"),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetProtoLoaderWithDuplicatedConfiguration",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "test_struct_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/codegen/io/testing/test_native_struct.h",
        "arolla/codegen/io/testing/test_proto_qtype.h",
    ],
    accessors = [
        protopath_accessor("&::x"),
        protopath_accessor(
            "&::str",
            "str",
            type = io_type.text,
        ),
        protopath_accessor(
            "&::raw_bytes",
            "raw_bytes",
        ),
        protopath_accessor("&::inner/*/&::a"),
        protopath_accessor("&::inner_proto/a"),
        protopath_accessor("&::inner_proto/&"),
    ],
    input_cls = "::testing_namespace::RootNativeStruct",
    loader_name = "::my_namespace::GetStructLoader",
    deps = [
        ":test_native_struct",
        ":test_proto_qtype",
    ],
)

cc_test(
    name = "test_struct_input_loader_test",
    srcs = ["test_struct_input_loader_test.cc"],
    deps = [
        ":test_native_struct",
        ":test_proto_qtype",
        ":test_struct_input_loader",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

input_loader(
    name = "test_span_proto_input_loader",
    testonly = 1,
    hdrs = ["arolla/proto/test.pb.h"],
    accessors = [
        protopath_accessor("[:]/x"),
        protopath_accessor("count([:])"),
        protopath_accessor(
            "[:]/str",
            "str",
            type = io_type.text,
        ),
    ],
    array_type = "DenseArray",
    input_cls = "absl::Span<const ::testing_namespace::Root>",
    loader_name = "::my_namespace::GetSpanProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

wildcard_input_loaders(
    name = "test_wildcard_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/codegen/io/testing/test_proto_qtype.h",
        "arolla/proto/test.pb.h",
    ],
    input_cls = "::testing_namespace::Root",
    loader_name2accessor = {
        "::my_namespace::GetWildcardMapIntProtoLoader": wildcard_protopath_accessor("map_int[*]"),
        "::my_namespace::GetWildcardMapInnerAProtoLoader": wildcard_protopath_accessor("map_string_inner[*]/a"),
        "::my_namespace::GetWildcardMapInnerAsProtoLoader": wildcard_protopath_accessor("map_string_inner[*]/as[:]"),
        "::my_namespace::GetWildcardMapInnerPtrProtoLoader": wildcard_protopath_accessor("map_string_inner[*]/&"),
        "::my_namespace::GetWildcardMapIntNoRenamingProtoLoader": wildcard_protopath_accessor(
            "map_int[*]",
            name = "%s",
        ),
        # It is fine to generate the same wildcard loader under another name, as long as no
        # combined_loader_name is provided.
        "::my_namespace::GetWildcardMapIntNoRenamingProtoLoader2": wildcard_protopath_accessor(
            "map_int[*]",
            name = "%s",
        ),
    },
    deps = [
        ":test_proto_qtype",
        "//arolla/proto:test_cc_proto",
    ],
)

# An input_loader with wildcard inputs.
input_loader(
    name = "input_loader_with_wildcards",
    testonly = 1,
    hdrs = ["arolla/proto/test.pb.h"],
    accessors = [
        protopath_accessor("/ys[:]"),
        wildcard_protopath_accessor("/map_int[*]"),
        wildcard_protopath_accessor("/map_string_inner[*]/a"),
        wildcard_protopath_accessor("/map_string_inner[*]/as[:]"),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetWildcardMapProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "test_proto_extension_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_extension.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "/Ext::testing_extension_namespace.extension_x_int32",
        ),
        protopath_accessor(
            "/inner/Ext::testing_extension_namespace.InnerExtension.inner_ext/inner_extension_x_int32",
        ),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetExtensionProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
    ],
)

input_loader(
    name = "test_repeated_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/codegen/io/testing/test_proto_qtype.h",
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "/ys[:]",
            "ys",
        ),
        protopath_accessor(
            "/inner/as[:]",
            "inner__as",
        ),
        protopath_accessor(
            "/inners[:]/as[:]",
            "inners__as",
        ),
        protopath_accessor(
            "/inners[:]/a",
            "inners__a",
        ),
        protopath_accessor(
            "/inners[:]/inner2/z",
            "inners__inner2__z",
        ),
        protopath_accessor(
            "/inners[1]/as[:]",
            "inners1__as",
        ),
        protopath_accessor(
            "/inners[:]/as[1]",
            "inners__as1",
        ),
        protopath_accessor(
            "/inners[:]/str",
            "inners__str",
            type = io_type.text,
        ),
        protopath_accessor(
            "/inners[:]/raw_bytes",
            "inners__raw_bytes",
            type = io_type.bytes,
        ),
        protopath_accessor(
            "/count(inners[:])",
            "inners_size",
        ),
        protopath_accessor(
            "/inners[:]/count(as[:])",
            "inners__as_size",
        ),
        protopath_accessor("inners[:]/&"),
        protopath_accessor(
            "/map_inner[:]/@key",
            "map_inner__keys",
        ),
        protopath_accessor(
            "/map_inner[:]/@value/a",
            "map_inner__value_a",
        ),
        protopath_accessor(
            "/count(map_inner[:])",
            "map_inner_size",
        ),
        # we are testing completely independent field that shouldn't
        # interact with any other field in generated code.
        # E.g., there will be no common intermediate results.
        protopath_accessor(
            "/self_reference/self_reference/self_reference/inners[:]/as[:]",
            "sr3_inners_as",
        ),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetRepeatedProtoLoader",
    deps = [
        ":test_proto_qtype",
        "//arolla/proto:test_cc_proto",
    ],
)

# We are testing special cases, when nodes for intermediate collection
# is the tree without branches, but actual nodes are branching.
input_loader(
    name = "test_repeated_proto_input_loader_with_no_branch_for_intemediate",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_extension.pb.h",
    ],
    accessors = [
        # nodes for intermediate collection:
        # 1. inners[:]
        # 2. inners[:]/root_reference/Ext::testing_extension_namespace.root_reference
        protopath_accessor(
            "/inners[:]/a",
            "inners/a",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/x",
            "inners/rr/x",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/Ext::testing_extension_namespace.root_reference/x",
            "inners/rr/sr/x",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/Ext::testing_extension_namespace.root_reference/x_int64",
            "inners/rr/sr/x64",
        ),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetRepeatedProtoLoaderWithNoBranchesForIntermediate",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
    ],
)

# Special case, when we avoid collection of intermediate node and start from
# the ancestor for performance reasons.
input_loader(
    name = "test_repeated_proto_input_loader_with_parent_intermediate_node_collected",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        # nodes for intermediate collection:
        # 1. inners[:]/root_reference
        # We should not collect cheap to access elements:
        # 1. inners[:]/root_reference/inner
        # 2. inners[:]/root_reference/inners[0]
        # 3. inners[:]/root_reference/inners[1]
        protopath_accessor(
            "/inners[:]/root_reference/inner/a",
            "inners/rr/inner/a",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/inner/as[0]",
            "inners/rr/inner/a0",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/inners[0]/a",
            "inners/rr/inners0/a",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/inners[0]/as[0]",
            "inners/rr/inners0/a0",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/inners[1]/a",
            "inners/rr/inners1/a",
        ),
        protopath_accessor(
            "/inners[:]/root_reference/inners[1]/as[0]",
            "inners/rr/inners1/a0",
        ),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetRepeatedProtoLoaderWithParentIntermediateNodeCollection",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "test_dense_array_repeated_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "/ys[:]",
            "ys",
        ),
        protopath_accessor(
            "/inner/as[:]",
            "inner__as",
        ),
        protopath_accessor(
            "/inners[:]/str",
            "inners__str",
            io_type.text,
        ),
        protopath_accessor(
            "/inners[:]/raw_bytes",
            "inners__raw_bytes",
        ),
        protopath_accessor(
            "/count(inners[:])",
            "inners_size",
        ),
        protopath_accessor(
            "/inners[:]/count(as[:])",
            "inners__as_size",
        ),
        protopath_accessor(
            "/inners[:]/as[:]",
            "inners__as",
        ),
        protopath_accessor(
            "/inners[:]/a",
            "inners__a",
        ),
        protopath_accessor(
            "/inners[:]/inner2/z",
            "inners__inner2__z",
        ),
    ],
    array_type = "DenseArray",
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetDenseArrayRepeatedProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

# special case single top level repeated field
# no intermediate result collection needed
input_loader(
    name = "test_dense_array_single_field_repeated_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "/ys[:]",
            "ys",
        ),
    ],
    array_type = "DenseArray",
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetDenseArraySingleFieldRepeatedProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

cc_test(
    name = "test_proto_input_loader_test",
    srcs = ["test_proto_input_loader_test.cc"],
    deps = [
        ":test_dense_array_repeated_proto_input_loader",
        ":test_dense_array_single_field_repeated_proto_input_loader",
        ":test_proto_extension_input_loader",
        ":test_proto_input_loader",
        ":test_proto_input_loader_with_duplicated_configuration",
        ":test_proto_qtype",
        ":test_repeated_proto_input_loader",
        ":test_repeated_proto_input_loader_with_no_branch_for_intemediate",
        ":test_repeated_proto_input_loader_with_parent_intermediate_node_collected",
        ":test_scalar_accessor_with_default_value_input_loader",
        ":test_span_proto_input_loader",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

input_loader(
    name = "benchmark_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [protopath_accessor("/x%d" % i) for i in range(10)],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetBenchProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "benchmark_nested_proto_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [protopath_accessor("/inner/inner2/root_reference/x%d" % i) for i in range(10)],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetBenchNestedProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "benchmark_nested_proto_with_extension_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_benchmark_extension.pb.h",
    ],
    accessors = [
        protopath_accessor("Ext::testing_extension_namespace.BenchmarkExtension.bench_ext/x%d" % i)
        for i in range(10)
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetBenchNestedProtoWithExtensionsLoader",
    deps = [
        "//arolla/proto:test_benchmark_extension_cc_proto",
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "benchmark_dense_array_nested_proto_with_extension_input_loader",
    testonly = 1,
    hdrs = [
        "absl/types/span.h",
        "arolla/proto/test.pb.h",
        "arolla/proto/test_benchmark_extension.pb.h",
    ],
    accessors = [protopath_accessor("[:]/Ext::testing_extension_namespace.BenchmarkExtension.bench_ext/x%d" % i) for i in range(10)],
    input_cls = "absl::Span<const ::testing_namespace::Root>",
    loader_name = "::my_namespace::GetBenchBatchNestedProtoWithExtensionsLoader",
    deps = [
        "//arolla/proto:test_benchmark_extension_cc_proto",
        "//arolla/proto:test_cc_proto",
        "@com_google_absl//absl/types:span",
    ],
)

input_loader(
    name = "benchmark_proto_string_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor("str"),
        protopath_accessor("raw_bytes"),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetBenchProtoStringLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader(
    name = "benchmark_array_input_loader",
    testonly = 1,
    hdrs = [],
    accessors = [path_accessor(
        "[%d]" % i,
        name = "x%d" % i,
    ) for i in range(10)],
    input_cls = "::std::array<int, 10>",
    loader_name = "::my_namespace::GetBenchArrayProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

cc_binary(
    name = "input_loader_benchmarks",
    testonly = 1,
    srcs = ["input_loader_benchmarks.cc"],
    deps = [
        ":benchmark_array_input_loader",
        ":benchmark_dense_array_nested_proto_with_extension_input_loader",
        ":benchmark_nested_proto_input_loader",
        ":benchmark_nested_proto_with_extension_input_loader",
        ":benchmark_proto_input_loader",
        ":benchmark_proto_string_input_loader",
        ":test_dense_array_repeated_proto_input_loader",
        ":test_descriptor_input_loader",
        ":test_repeated_proto_input_loader_with_parent_intermediate_node_collected",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/proto/testing",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "input_loader_benchmarks_smoke_test",
    binary = ":input_loader_benchmarks",
)

cc_binary(
    name = "raw_proto_access_benchmarks",
    testonly = 1,
    srcs = ["raw_proto_access_benchmarks.cc"],
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "raw_proto_access_benchmarks_smoke_test",
    binary = ":raw_proto_access_benchmarks",
)

cc_binary(
    name = "benchmarks_binary_size_baseline",
    testonly = 1,
    srcs = ["benchmarks_binary_size_baseline.cc"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "test_proto_slot_listener_test",
    srcs = ["test_proto_slot_listener_test.cc"],
    deps = [
        ":test_array_proto_slot_listener",
        ":test_proto_slot_listener",
        ":test_sized_slot_listener",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto",
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "test_empty_slot_listener_test",
    srcs = ["test_empty_slot_listener_test.cc"],
    deps = [
        ":test_empty_slot_listener",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "wildcard_proto_input_loader_test",
    srcs = ["wildcard_proto_input_loader_test.cc"],
    deps = [
        ":test_proto_qtype",
        ":test_wildcard_proto_input_loader",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "input_loader_with_wildcards_test",
    srcs = ["input_loader_with_wildcards_test.cc"],
    deps = [
        ":input_loader_with_wildcards",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

slot_listener(
    name = "test_struct_slot_listener",
    testonly = 1,
    hdrs = ["arolla/codegen/io/testing/test_native_struct.h"],
    accessors = [
        protopath_accessor("&::x"),
        protopath_accessor(
            "&::str",
            "str",
            type = io_type.text,
        ),
        protopath_accessor(
            "&::raw_bytes",
            "raw_bytes",
        ),
        protopath_accessor("&::inner/*/&::a"),
        protopath_accessor("&::inner_proto/a"),
    ],
    output_cls = "::testing_namespace::RootNativeStruct",
    slot_listener_name = "::my_namespace::GetStructListener",
    deps = [
        ":test_native_struct",
    ],
)

cc_test(
    name = "test_struct_slot_listener_test",
    srcs = ["test_struct_slot_listener_test.cc"],
    deps = [
        ":test_native_struct",
        ":test_struct_slot_listener",
        "//arolla/memory",
        "//arolla/naming",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

slot_listener(
    name = "test_proto_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_extension.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Root",
            proto_extension_modules = [
                "arolla.proto.test_extension_pb2",
            ],
            py_deps = [
                "//arolla/proto:test_py_proto",
                "//arolla/proto:test_extension_py_proto",
            ],
        ),
        protopath_accessor(
            "str",
            "str",
            type = io_type.text,
        ),
        protopath_accessor(
            "raw_bytes",
            "raw_bytes",
        ),
        protopath_accessor(
            "map_int['a']",
            "map_int_a",
        ),
        protopath_accessor(
            "inner/a",
            "inner__a",
        ),
        protopath_accessor(
            "ys[0]",
            "ys_0",
        ),
        protopath_accessor(
            "inners[0]/a",
            "inners_0__a",
        ),
    ],
    array_type = "DenseArray",
    output_cls = call_python_function(
        "arolla.codegen.io.testing.test_util.make_cpp_name",
        [
            "testing_namespace",
            "Root",
        ],
        deps = [":test_util"],
    ),
    slot_listener_name = "::my_namespace::GetProtoSlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

slot_listener(
    name = "test_sized_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor("inners[:]/a"),
        protopath_accessor("count(inners[:])"),
        protopath_accessor("inners[:]/inners2[:]/z"),
        protopath_accessor("inners[:]/count(inners2[:])"),
    ],
    array_type = "DenseArray",
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetProtoSizedSlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

slot_listener(
    name = "test_slot_listener_filtered_for_model",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [filtered_for_models_accessor(
        [
            protopath_accessor("x0", "plus1_bit0"),
            protopath_accessor("x1", "bit0"),
            protopath_accessor("x2", "plus1"),
            protopath_accessor("x3", "bit0_plus1"),
            protopath_accessor("x4", "self_bit0"),
            protopath_accessor("x5", "self"),
            protopath_accessor("x6", "bit0_self"),
            protopath_accessor("x7", "double"),
        ],
        [
            "self_bit0.io.textproto",
            "bit0_plus1.io.textproto",
        ],
    )],
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetFilteredSlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

slot_listener(
    name = "test_array_proto_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "inners[:]/a",
            "inners__a",
        ),
        protopath_accessor(
            "inner/inners2[:]/z",
            "inners2__z",
        ),
        protopath_accessor(
            "inners[:]/inners2[:]/z",
            "inners__inners2__z",
        ),
        protopath_accessor(
            "inners[:]/root_reference/map_string_inner['a']/a",
            "in_map_a",
        ),
        protopath_accessor(
            "inners[:]/inners2[0]/z",
            "in_array_z",
        ),
        protopath_accessor(
            "inners[:]/as[0]",
            "in_array_as",
        ),
    ],
    array_type = "DenseArray",
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetArrayProtoSlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_extension_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

slot_listener(
    name = "test_proto_all_arrays_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_proto3.pb.h",
    ],
    accessors = [
        proto_fields_accessor(
            "arolla.proto.test_pb2.Root",
            py_deps = [
                "//arolla/proto:test_py_proto",
            ],
        ),
    ],
    array_type = "DenseArray",
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetAllArrayProtoSlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
        "//arolla/proto:test_proto3_cc_proto",
    ],
)

slot_listener(
    name = "test_empty_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [],
    array_type = "DenseArray",
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetEmptySlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

slot_listener(
    name = "benchmark_proto_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [protopath_accessor("x%d" % i) for i in range(10)],
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetBenchProtoSlotListener",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

slot_listener(
    name = "benchmark_nested_proto_with_extension_slot_listener",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
        "arolla/proto/test_benchmark_extension.pb.h",
    ],
    accessors = [
        protopath_accessor("self_reference/Ext::testing_extension_namespace.BenchmarkExtension.bench_ext/x%d" % i)
        for i in range(10)
    ],
    output_cls = "::testing_namespace::Root",
    slot_listener_name = "::my_namespace::GetBenchNestedProtoWithExtensionsSlotListener",
    deps = [
        "//arolla/proto:test_benchmark_extension_cc_proto",
        "//arolla/proto:test_cc_proto",
    ],
)

cc_binary(
    name = "slot_listener_benchmarks",
    testonly = 1,
    srcs = ["slot_listener_benchmarks.cc"],
    deps = [
        ":benchmark_nested_proto_with_extension_slot_listener",
        ":benchmark_proto_slot_listener",
        ":test_proto_slot_listener",
        "//arolla/dense_array/qtype",
        "//arolla/io/proto/testing",
        "//arolla/proto:test_cc_proto",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

benchmark_smoke_test(
    name = "slot_listener_benchmarks_smoke_test",
    binary = ":slot_listener_benchmarks",
)

py_library(
    name = "accessor_generators",
    testonly = 1,
    srcs = ["accessor_generators.py"],
    deps = ["//arolla/codegen/io"],
)

input_loader(
    name = "test_generator_based_input_loader",
    testonly = 1,
    hdrs = [],
    accessors = [
        accessor_generator(
            call_python_function(
                "arolla.codegen.io.testing.accessor_generators.gen_zero_one",
                args = [],
                deps = [":accessor_generators"],
            ),
        ),
        accessor_generator(
            call_python_function(
                "arolla.codegen.io.testing.accessor_generators.gen_from_args",
                args = [
                    "2",
                    "3",
                    "4",
                ],
                deps = [":accessor_generators"],
            ),
        ),
        accessor_generator(
            call_python_function(
                "arolla.codegen.io.testing.accessor_generators.gen_from_data_file",
                args = ["$(execpath :generator_indices.txt)"],
                data = [":generator_indices.txt"],
                deps = [":accessor_generators"],
            ),
        ),
    ],
    input_cls = "::std::array<int, 10>",
    loader_name = "::my_namespace::GetAccessorsGeneratedArrayLoader",
)

input_loader(
    name = "test_scalar_accessor_with_default_value_input_loader",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        accessor_generator(
            call_python_function(
                "arolla.codegen.io.testing.accessor_generators.gen_scalar_accessor_with_default_value",
                args = [],
                deps = [":accessor_generators"],
            ),
        ),
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace::GetSingleValueWithDefaultProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

input_loader_set(
    name = "test_input_loader_set",
    testonly = 1,
    loaders_spec = call_python_function(
        "arolla.codegen.io.testing.accessor_generators.gen_input_loader_set_spec",
        args = [],
        deps = [":accessor_generators"],
    ),
)

input_loader_set(
    name = "test_input_loader_set_spec_by_value",
    testonly = 1,
    loaders_spec = {
        "::my_namespace::ShardedLoader": {
            "accessors": test_accessors,
            "input_cls": "int",
            "sharding": sharding_info(shard_count = 3),
            "hdrs": [],
        },
        "::my_namespace::NonShardedLoader": {
            "accessors": test_accessors,
            "input_cls": "int",
            "hdrs": [],
        },
    },
    max_shard_count = 4,  # last file will be empty
)

input_loader_set(
    name = "test_input_loader_set_call_spec_py_func_value",
    testonly = 1,
    loaders_spec = {
        "::my_namespace::LoaderFromPyFunc": {
            "accessors": test_accessors,
            "input_cls": call_python_function(
                "arolla.codegen.io.testing.accessor_generators.gen_input_loader_set_input_cls",
                args = [],
                deps = [":accessor_generators"],
            ),
            "hdrs": [],
        },
    },
)

slot_listener_set(
    name = "test_slot_listener_set",
    testonly = 1,
    listeners_spec = call_python_function(
        "arolla.codegen.io.testing.accessor_generators.gen_slot_listener_set_spec",
        args = [],
        deps = [":accessor_generators"],
    ),
)

cc_test(
    name = "test_slot_listener_set_test",
    srcs = ["test_slot_listener_set_test.cc"],
    deps = [
        ":test_slot_listener_set",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

# Stress test for compilation and stack usage for single value proto input loader with many inputs
input_loader(
    name = "test_proto_single_value_input_loader_stress",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "self_reference/" * sr_depth + "inners[%s]/root_reference/" % inners_idx + "x%d" % i,
            name = "x%d_%d_%d" % (sr_depth, inners_idx, i),
        )
        for i in range(10)
        for sr_depth in range(10)
        for inners_idx in range(10)
    ],
    input_cls = "::testing_namespace::Root",
    loader_name = "::my_namespace_stress::GetSingleValueProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

# Stress test for compilation and stack usage for multi value proto input loader with many inputs
input_loader(
    name = "test_proto_multi_value_input_loader_stress",
    testonly = 1,
    hdrs = [
        "arolla/proto/test.pb.h",
    ],
    accessors = [
        protopath_accessor(
            "[:]/" + "self_reference/" * sr_depth + "inners[%s]/inners2[:]/root_reference/" % inners_idx + "x%d" % i,
            name = "x%d_%d_%d" % (sr_depth, inners_idx, i),
        )
        for i in range(10)
        for sr_depth in range(10)
        for inners_idx in range(10)
    ],
    input_cls = "absl::Span<const ::testing_namespace::Root>",
    loader_name = "::my_namespace_stress::GetMultiValueProtoLoader",
    deps = [
        "//arolla/proto:test_cc_proto",
    ],
)

cc_test(
    name = "input_loader_stress_test",
    srcs = ["input_loader_stress_test.cc"],
    deps = [
        ":test_input_loader_compilation_stress",
        ":test_input_loader_compilation_stress_sharded",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "input_loader_initialization_stress_test",
    srcs = ["input_loader_initialization_stress_test.cc"],
    deps = [
        ":test_proto_multi_value_input_loader_stress",
        ":test_proto_single_value_input_loader_stress",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/proto:test_cc_proto",
        "//arolla/qtype",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "input_loader_set_test",
    srcs = ["input_loader_set_test.cc"],
    deps = [
        ":test_input_loader_set_spec_by_value",
        "//arolla/io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)
