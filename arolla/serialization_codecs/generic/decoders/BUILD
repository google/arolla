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

# NOTE: Consider splitting this target to subtarget to provide
# better control on what codecs get included to the binary.

# See tests in py/arolla/s11n/testing
cc_library(
    name = "decoders",
    srcs = [
        "operator_decoder.cc",
        "optional_decoder.cc",
        "scalar_decoder.cc",
        "tuple_decoder.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr",
        "//arolla/expr/operator_loader",
        "//arolla/expr/operators/while_loop",
        "//arolla/memory",
        "//arolla/qtype",
        "//arolla/qtype/array_like",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs:registry",
        "//arolla/serialization_codecs/generic:codec_name",
        "//arolla/serialization_codecs/generic:operator_codec_cc_proto",
        "//arolla/serialization_codecs/generic:optional_codec_cc_proto",
        "//arolla/serialization_codecs/generic:scalar_codec_cc_proto",
        "//arolla/serialization_codecs/generic:tuple_codec_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf_lite",
    ],
    alwayslink = True,
)
