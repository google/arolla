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

package(default_visibility = ["//arolla:internal"])

licenses(["notice"])

# This shared library includes the Python C++ API for Arolla:
#   //py/arolla/abc:py_abc.
cc_shared_library(
    name = "arolla_py_abc_so",
    dynamic_deps = [
        ":arolla_so",
        ":base_so",
    ],
    exports_filter = ["//py/arolla:__subpackages__"],
    user_link_flags = select({
        "@platforms//os:macos": ["-Wl,-undefined,dynamic_lookup"],
        "//conditions:default": [],
    }),
    visibility = ["//visibility:public"],
    deps = ["//py/arolla/abc:py_abc"],
)

# This shared library includes the standard operator package.
cc_shared_library(
    name = "arolla_standard_operators_so",
    dynamic_deps = [
        ":arolla_so",
        ":base_so",
    ],
    exports_filter = [
        "//arolla/expr/operators:__subpackages__",
        "//arolla/qexpr/operators:__subpackages__",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
    ],
)

# This shared library includes the Arolla C++ library without the standard
# operator package.
#
cc_shared_library(
    name = "arolla_so",
    dynamic_deps = [":base_so"],
    exports_filter = [
        "//arolla:__subpackages__",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//arolla/codegen/operator_package",
        "//arolla/codegen/operator_package:operator_package_cc_proto",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/optimization",
        "//arolla/expr/optimization/default",
        "//arolla/expr/optimization/peephole_optimizations",
        "//arolla/expr/visitors",
        "//arolla/io",
        "//arolla/jagged_shape/array/qtype",
        "//arolla/jagged_shape/dense_array/qtype",
        "//arolla/jagged_shape/qtype",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "//arolla/serialization",
        "//arolla/serialization_codecs:all",
        "//arolla/util",
    ],
)

# This shared library includes all the dependencies of the Arolla C++ library.
#
# NOTE: This target is currently in an experimental state. Specifically, we are
# uncertain whether it should serve as a provider of Abseil and Protobuf to
# other libraries that use the Arolla Python API.
#
cc_shared_library(
    name = "base_so",
    exports_filter = [
        "@com_google_absl//:__subpackages__",
        "@com_google_cityhash//:__subpackages__",
        "@com_google_protobuf//:__subpackages__",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:dynamic_annotations",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/cleanup",
        "@com_google_absl//absl/container:fixed_array",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/debugging:leak_check",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/log:die_if_null",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_cityhash//:cityhash",
        "@com_google_double_conversion//:double-conversion",
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//src/google/protobuf/io",
        "@com_google_protobuf//src/google/protobuf/io:gzip_stream",
        "@icu4c",
    ],
)
