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
#ifndef AROLLA_IO_PROTO_REFLECTION_READER_H_
#define AROLLA_IO_PROTO_REFLECTION_READER_H_

#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla::proto {

// Encapsulates different type of reading proto field into the arolla::Frame.
struct RegularFieldAccess {};
struct RepeatedFieldIndexAccess {
  explicit RepeatedFieldIndexAccess(size_t idx) : idx(idx) {}
  size_t idx;
};
struct RepeatedFieldAccess {};
struct RepeatedFieldSizeAccess {};

using ProtoFieldAccessInfo =
    std::variant<RegularFieldAccess,  // default state
                 RepeatedFieldIndexAccess, RepeatedFieldAccess,
                 RepeatedFieldSizeAccess>;

class ProtoTypeReader {
 public:
  using BoundReadFn = std::function<void(const google::protobuf::Message&, FramePtr)>;

  // Creates a reader reading to the OptionalValue.
  // Reader doesn't respect proto default values.
  // Returns an error if last field type() is not supported (e.g., TYPE_MESSAGE)
  // All intermediate fields must be TYPE_MESSAGE.
  // All access infos must be one of:
  // RegularFieldAccess, RepeatedFieldIndexAccess
  //
  // access_infos is specifying type of the access and additional information
  // to read from each of the field.
  // access_infos must have the same size as fields.
  static absl::StatusOr<std::unique_ptr<ProtoTypeReader>> CreateOptionalReader(
      absl::Span<const google::protobuf::FieldDescriptor* const> fields,
      std::vector<ProtoFieldAccessInfo> access_infos,
      proto::StringFieldType string_type = StringFieldType::kText);

  // Creates a reader reading to the DenseArrayShape.
  // Returns an error if last field type() is not repeated
  // All access infos except the last must be one of:
  // RegularFieldAccess, RepeatedFieldIndexAccess
  // Last access info must be RepeatedFieldSizeAccess.
  //
  // access_infos is specifying type of the access and additional information
  // to read from each of the field.
  // access_infos must have the same size as fields.
  static absl::StatusOr<std::unique_ptr<ProtoTypeReader>>
  CreateDenseArrayShapeReader(
      absl::Span<const google::protobuf::FieldDescriptor* const> fields,
      std::vector<ProtoFieldAccessInfo> access_infos,
      proto::StringFieldType string_type = StringFieldType::kText);

  // Creates a reader reading to the arolla::DenseArray.
  // Reader doesn't respect proto default values.
  // Returns an error if last field type() is not supported (e.g., TYPE_MESSAGE)
  // All intermediate fields must be TYPE_MESSAGE.
  //
  // access_infos is specifying type of the access and additional information
  // to read from each of the field.
  // access_infos must have the same size as fields.
  // If all accesses are RegularFieldAccess or RepeatedFieldIndexAccess,
  // DenseArray with a single element returned.
  // If no RepeatedFieldSizeAccess specified, arolla::DenseArray<T> will
  // be read.
  // RepeatedFieldSizeAccess is allowed only as a last element. It is only
  // applicable for the repeated field. In such case
  // arolla::DenseArray<int32_t> is returned.
  static absl::StatusOr<std::unique_ptr<ProtoTypeReader>>
  CreateDenseArrayReader(
      absl::Span<const google::protobuf::FieldDescriptor* const> fields,
      std::vector<ProtoFieldAccessInfo> access_infos,
      proto::StringFieldType string_type = StringFieldType::kText);

  ProtoTypeReader(
      QTypePtr qtype,
      std::function<absl::StatusOr<BoundReadFn>(TypedSlot)> read_fn_factory)
      : qtype_(qtype), read_fn_factory_(std::move(read_fn_factory)) {}

  // Returns expected output QType.
  QTypePtr qtype() const { return qtype_; }

  // Returns function to read from proto to the specified slot.
  // Returns error if slot has type different from qtype().
  absl::StatusOr<BoundReadFn> BindReadFn(TypedSlot slot) const {
    return read_fn_factory_(slot);
  }

 private:
  QTypePtr qtype_;
  std::function<absl::StatusOr<BoundReadFn>(TypedSlot)> read_fn_factory_;
};

}  // namespace arolla::proto

#endif  // AROLLA_IO_PROTO_REFLECTION_READER_H_
