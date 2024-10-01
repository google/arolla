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
#ifndef AROLLA_IO_PROTO_PROTO_INPUT_LOADER_H_
#define AROLLA_IO_PROTO_PROTO_INPUT_LOADER_H_

#include <string>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Dynamic loader from `google::protobuf::Message` based on proto reflection.
class ProtoFieldsLoader : public InputLoader<google::protobuf::Message> {
  struct PrivateConstructorTag {};

 public:
  // Constructs InputLoader for the proto message descriptor.
  //
  // `google::protobuf::Descriptor` pointer is stored inside of `ProtoFieldsLoader`
  // and `BoundInputLoader`. `google::protobuf::Descriptor` MUST outlive both of them.
  //
  // The resulting BoundInputLoader only accepts `google::protobuf::Message` created using
  // exactly the same google::protobuf::Descriptor (i. e. `google::protobuf::Message` must be
  // created from the same `DescriptorPool`, note that builtin protos all have
  // the same `DescriptorPool`).
  //
  // The input names are threated as XPaths with the following behavior:
  //  `/foo`: selects the child named "foo" of the left-side message.
  //  `/foo/@size` : counts elements in the repeated field "foo". It must
  //     be the last element in path. If there is no other repeated fields in
  //     the path DenseArrayShape object will be returned. Otherwise
  //     DenseArray<::arolla::proto::arolla_size_t> type will be returned.
  //  `foo[i]`: selects `i`th element from in the repeated field "foo".
  //
  // Not yet supported operators:
  //   `foo["key"]`: selects element with "key" from a map<string, T> field.
  //   `foo/@keys`: selects all sorted keys in the map.
  //   `foo/@values`: selects all values sorted by key in the map.
  //
  static absl::StatusOr<InputLoaderPtr<google::protobuf::Message>> Create(
      const google::protobuf::Descriptor* descr,
      proto::StringFieldType string_type = proto::StringFieldType::kText);

  absl::Nullable<const QType*> GetQTypeOf(absl::string_view name) const final;
  std::vector<std::string> SuggestAvailableNames() const final;

  // private
  explicit ProtoFieldsLoader(PrivateConstructorTag,
                             const google::protobuf::Descriptor* descr,
                             proto::StringFieldType string_type);

 private:
  // Bind to a particular slots.
  // Returns an error if slot doesn't match GetOutputTypes.
  absl::StatusOr<BoundInputLoader<google::protobuf::Message>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const override;

  const google::protobuf::Descriptor* descr_;
  proto::StringFieldType string_type_;
};

}  // namespace arolla

#endif  // AROLLA_IO_PROTO_PROTO_INPUT_LOADER_H_
