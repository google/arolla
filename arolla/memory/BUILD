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
    name = "memory",
    srcs = [
        "frame.cc",
        "optional_value.cc",
        "raw_buffer_factory.cc",
        "simple_buffer.h",
        "strings_buffer.cc",
        "strings_buffer.h",
        "void_buffer.h",
    ],
    hdrs = [
        "buffer.h",
        "frame.h",
        "memory_allocation.h",
        "optional_value.h",
        "raw_buffer_factory.h",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:dynamic_annotations",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:btree",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_test(
    name = "frame_benchmark",
    srcs = ["frame_benchmark.cc"],
    deps = [
        ":memory",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
    ],
)

#
# Unittests
#
cc_test(
    name = "buffer_test",
    size = "small",
    srcs = ["buffer_test.cc"],
    deps = [
        ":memory",
        "//arolla/qtype",
        "//arolla/util",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "raw_buffer_factory_test",
    size = "small",
    srcs = ["raw_buffer_factory_test.cc"],
    deps = [
        ":memory",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_test(
    name = "strings_buffer_test",
    size = "small",
    srcs = ["strings_buffer_test.cc"],
    deps = [
        ":memory",
        "//arolla/util",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "void_buffer_test",
    size = "small",
    srcs = ["void_buffer_test.cc"],
    deps = [
        ":memory",
        "@com_google_absl//absl/hash:hash_testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "frame_test",
    srcs = ["frame_test.cc"],
    deps = [
        ":memory",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/base:dynamic_annotations",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "memory_allocation_test",
    srcs = ["memory_allocation_test.cc"],
    deps = [
        ":memory",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "optional_value_test",
    srcs = ["optional_value_test.cc"],
    deps = [
        ":memory",
        "//arolla/util",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_googletest//:gtest_main",
    ],
)
