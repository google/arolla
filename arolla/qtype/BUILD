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

load("//arolla/util/testing:testing.bzl", "benchmark_smoke_test")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "qtype",
    srcs = [
        "any_qtype.cc",
        "base_types.cc",
        "derived_qtype.cc",
        "named_field_qtype.cc",
        "optional_qtype.cc",
        "qtype.cc",
        "shape_qtype.cc",
        "simple_qtype.cc",
        "slice_qtype.cc",
        "tuple_qtype.cc",
        "typed_ref.cc",
        "typed_slot.cc",
        "typed_value.cc",
        "unspecified_qtype.cc",
        "weak_qtype.cc",
    ],
    hdrs = [
        "any_qtype.h",
        "base_types.h",
        "derived_qtype.h",
        "named_field_qtype.h",
        "optional_qtype.h",
        "qtype.h",
        "qtype_traits.h",
        "shape_qtype.h",
        "simple_qtype.h",
        "slice_qtype.h",
        "tuple_qtype.h",
        "typed_ref.h",
        "typed_slot.h",
        "typed_value.h",
        "unspecified_qtype.h",
        "weak_qtype.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/memory",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:distributions",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
    ],
)

cc_test(
    name = "qtype_test",
    srcs = [
        "optional_qtype_test.cc",
        "qtype_test.cc",
        "simple_qtype_test.cc",
        "slice_qtype_test.cc",
        "tuple_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        ":qtype_test_utils",
        "//arolla/memory",
        "//arolla/qtype/testing",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "any_qtype_test",
    srcs = [
        "any_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "derived_qtype_test",
    srcs = [
        "derived_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "unspecified_qtype_test",
    srcs = [
        "unspecified_qtype_test.cc",
    ],
    deps = [
        ":qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "typed_slot_test",
    srcs = ["typed_slot_test.cc"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "qtype_test_utils",
    testonly = 1,
    hdrs = ["qtype_test_utils.h"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "@com_google_googletest//:gtest",
    ],
)

cc_binary(
    name = "qtype_benchmark",
    testonly = 1,
    srcs = ["qtype_benchmark.cc"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

# There is no tests in qtype_benchmark.cc. It only checks that the benchmark is compilable.
benchmark_smoke_test(
    name = "qtype_benchmark_smoke_test",
    binary = ":qtype_benchmark",
)

cc_test(
    name = "typed_value_test",
    srcs = ["typed_value_test.cc"],
    deps = [
        ":qtype",
        "//arolla/memory",
        "//arolla/util",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "shape_qtype_test",
    srcs = ["shape_qtype_test.cc"],
    deps = [
        ":qtype",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "qtype_traits_test",
    srcs = ["qtype_traits_test.cc"],
    deps = [
        ":qtype",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "weak_qtype_test",
    srcs = ["weak_qtype_test.cc"],
    deps = [
        ":qtype",
        "//arolla/array/qtype",
        "//arolla/memory",
        "//arolla/qtype/standard_type_properties",
        "//arolla/util",
        "//arolla/util/testing",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)
