// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#include <cstdint>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "google/protobuf/descriptor.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/bytes.h"

namespace arolla::proto {

using ::arolla::GetQType;
using ::arolla::QTypePtr;
using ::google::protobuf::FieldDescriptor;

absl::StatusOr<QTypePtr> ProtoFieldTypeToQType(
    FieldDescriptor::Type field_type) {
  // Mostly follow the mapping between Type and CppType specified in
  // protobuf/descriptor_lite.h
  switch (field_type) {
    case FieldDescriptor::TYPE_INT32:
    case FieldDescriptor::TYPE_SINT32:
    case FieldDescriptor::TYPE_SFIXED32:
      return GetQType<arolla_single_value_t<int32_t>>();
    case FieldDescriptor::TYPE_INT64:
    case FieldDescriptor::TYPE_SINT64:
    case FieldDescriptor::TYPE_SFIXED64:
      return GetQType<arolla_single_value_t<int64_t>>();
    case FieldDescriptor::TYPE_UINT32:
    case FieldDescriptor::TYPE_FIXED32:
      return GetQType<arolla_single_value_t<uint32_t>>();
    case FieldDescriptor::TYPE_UINT64:
    case FieldDescriptor::TYPE_FIXED64:
      return GetQType<arolla_single_value_t<uint64_t>>();
    case FieldDescriptor::TYPE_DOUBLE:
      return GetQType<arolla_single_value_t<double>>();
    case FieldDescriptor::TYPE_FLOAT:
      return GetQType<arolla_single_value_t<float>>();
    case FieldDescriptor::TYPE_BOOL:
      return GetQType<arolla_single_value_t<bool>>();
    case FieldDescriptor::TYPE_STRING:
      // TODO: Add an option to map TYPE_STRING to TEXT QType.
      return GetQType<arolla_single_value_t<std::string>>();
    case FieldDescriptor::TYPE_BYTES:
      return GetQType<arolla::Bytes>();
    case FieldDescriptor::TYPE_ENUM:
      return GetQType<arolla_single_value_t<int32_t>>();
    default:
      return absl::InvalidArgumentError(
          absl::StrCat("type ", field_type, " is not supported"));
  }
}

}  // namespace arolla::proto
