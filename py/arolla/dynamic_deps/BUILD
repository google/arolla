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

cc_shared_library(
    name = "py_abc_so",
    dynamic_deps = [
        ":arolla_so",
        ":base_so",
    ],
    exports_filter = ["//py/arolla:__subpackages__"],
    user_link_flags = select({
        "@platforms//os:macos": ["-Wl,-undefined,dynamic_lookup"],
        "//conditions:default": [],
    }),
    deps = ["//py/arolla/abc:py_abc"],
)

# FIXME: Temporarily included all the standard operators and
# serialization codecs in dependencies of the Arolla Python API.
cc_library(
    name = "temporary_extras",
    deps = [
        "//arolla/expr/operators/all",
        "//arolla/qexpr/operators/all",
        "//arolla/serialization_codecs:all",
    ],
)

cc_shared_library(
    name = "arolla_so",
    dynamic_deps = [":base_so"],
    exports_filter = [
        "//arolla:__subpackages__",
    ],
    deps = [
        ":temporary_extras",
        "//arolla/expr",
        "//arolla/expr/eval",
        "//arolla/expr/operators:bootstrap",
        "//arolla/expr/optimization",
        "//arolla/expr/visitors",
        "//arolla/io",
        "//arolla/memory",
        "//arolla/qexpr",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/qtype",
        "//arolla/serialization:decode",
        "//arolla/serialization:encode",
        "//arolla/util",
    ],
)

# NOTE: This target is currently in an experimental state. In particular, we
# shall avoid allowing it to become a source of Abseil and Protobuf dependencies
# for other libraries, and we plan to evolve it in that direction.
#
cc_shared_library(
    name = "base_so",
    exports_filter = [
        "@@abseil-cpp~//absl:__subpackages__",
        "@@protobuf~//:__subpackages__",
        "@@protobuf~~non_module_deps~utf8_range//:utf8_validity",
    ],
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
        "@com_google_protobuf//:protobuf",
        "@com_google_protobuf//src/google/protobuf/io",
        "@com_google_protobuf//src/google/protobuf/io:gzip_stream",
    ],
)
