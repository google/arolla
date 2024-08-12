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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "io",
    srcs = [
        "accessor_helpers.h",
        "input_loader.cc",
        "slot_listener.cc",
        "string_slot_listener.cc",
        "struct_io.cc",
        "typed_refs_input_loader.cc",
        "wildcard_input_loader.cc",
    ],
    hdrs = [
        "accessors_input_loader.h",
        "accessors_slot_listener.h",
        "chain_slot_listener.h",
        "delegating_input_loader.h",
        "delegating_slot_listener.h",
        "inplace_slot_builder.h",
        "input_loader.h",
        "slot_listener.h",
        "span_input_loader.h",
        "string_slot_listener.h",
        "struct_io.h",
        "tuple_input_loader.h",
        "typed_refs_input_loader.h",
        "wildcard_input_loader.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/log:die_if_null",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "input_loader_test",
    srcs = [
        "input_loader_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "slot_listener_test",
    srcs = [
        "slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "chain_slot_listener_test",
    srcs = [
        "chain_slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "accessor_helpers_test",
    srcs = [
        "accessor_helpers.h",
        "accessor_helpers_test.cc",
    ],
    deps = [
        ":io",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "accessors_input_loader_test",
    srcs = [
        "accessors_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "accessors_slot_listener_test",
    srcs = [
        "accessors_slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "inplace_slot_builder_test",
    srcs = [
        "inplace_slot_builder_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "delegating_input_loader_test",
    srcs = ["delegating_input_loader_test.cc"],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "delegating_slot_listener_test",
    srcs = ["delegating_slot_listener_test.cc"],
    deps = [
        ":io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "wildcard_input_loader_test",
    srcs = [
        "wildcard_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:optional",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "string_slot_listener_test",
    srcs = [
        "string_slot_listener_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "span_input_loader_test",
    srcs = [
        "span_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "tuple_input_loader_test",
    srcs = [
        "tuple_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "typed_refs_input_loader_test",
    srcs = [
        "typed_refs_input_loader_test.cc",
    ],
    deps = [
        ":io",
        "//arolla/io/testing",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "struct_io_test",
    srcs = ["struct_io_test.cc"],
    deps = [
        ":io",
        "//arolla/dense_array",
        "//arolla/dense_array/qtype",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "struct_io_benchmarks",
    testonly = 1,
    srcs = ["struct_io_benchmarks.cc"],
    deps = [
        ":io",
        "//arolla/memory",
        "//arolla/qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)
