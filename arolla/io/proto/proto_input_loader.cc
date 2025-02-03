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
#include "arolla/io/proto/proto_input_loader.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/base/nullability.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "absl/types/span.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/message.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/proto/reflection/reader.h"
#include "arolla/io/proto_types/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace {

using proto::ProtoTypeReader;
using proto::StringFieldType;

absl::StatusOr<std::unique_ptr<ProtoTypeReader>> CreateReaderWithStringType(
    absl::Span<const google::protobuf::FieldDescriptor* const> fields,
    std::vector<proto::ProtoFieldAccessInfo> access_infos,
    StringFieldType string_type) {
  auto repeated_it = std::find_if(
      access_infos.begin(), access_infos.end(),
      [](proto::ProtoFieldAccessInfo v) {
        return std::holds_alternative<proto::RepeatedFieldAccess>(v);
      });
  if (repeated_it == access_infos.end()) {
    if (!access_infos.empty() &&
        std::holds_alternative<proto::RepeatedFieldSizeAccess>(
            access_infos.back())) {
      return proto::ProtoTypeReader::CreateDenseArrayShapeReader(
          fields, std::move(access_infos), string_type);
    } else {
      return proto::ProtoTypeReader::CreateOptionalReader(
          fields, std::move(access_infos), string_type);
    }
  } else {
    return proto::ProtoTypeReader::CreateDenseArrayReader(
        fields, std::move(access_infos), string_type);
  }
}

// Returns field name and extra access information
absl::StatusOr<std::pair<std::string, proto::ProtoFieldAccessInfo>>
ParseProtopathElement(absl::string_view path_element) {
  bool is_size_element = absl::ConsumeSuffix(&path_element, "@size");
  if (!absl::StrContains(path_element, '[') &&
      !absl::StrContains(path_element, ']')) {
    if (is_size_element) {
      return std::pair{std::string(path_element),
                       proto::RepeatedFieldSizeAccess{}};
    } else {
      return std::pair{std::string(path_element),
                       proto::ProtoFieldAccessInfo{}};
    }
  }
  if (is_size_element) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "@size accessor does not accept field access by index, got %s",
        path_element));
  }
  // Parsing index access in form of "field_name[\d+]".
  std::vector<absl::string_view> splits =
      absl::StrSplit(path_element, absl::ByAnyChar("[]"), absl::SkipEmpty());
  auto error = [&]() {
    return absl::FailedPreconditionError(absl::StrCat(
        "cannot parse access by index protopath element: ", path_element));
  };
  if (splits.size() != 2) {
    return error();
  }
  std::string field_name(splits[0]);

  size_t idx = static_cast<size_t>(-1);
  if (!absl::SimpleAtoi(splits[1], &idx)) {
    return error();
  }
  if (absl::StrFormat("%s[%d]", field_name, idx) != path_element) {
    return error();
  }
  return std::pair{field_name, proto::RepeatedFieldIndexAccess{idx}};
}

absl::StatusOr<std::unique_ptr<proto::ProtoTypeReader>> ParseProtopathToReader(
    const google::protobuf::Descriptor* const descr, absl::string_view protopath,
    proto::StringFieldType string_type) {
  if (!absl::ConsumePrefix(&protopath, "/")) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "protopath must start with '/', got: \"%s\"", protopath));
  }
  std::vector<std::string> elements = absl::StrSplit(protopath, '/');
  if (elements.empty()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("empty protopath: %s", protopath));
  }
  if (elements.back() == "@size" && elements.size() > 1) {
    elements.pop_back();
    elements.back().append("@size");
  }
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  std::vector<proto::ProtoFieldAccessInfo> access_infos;
  const google::protobuf::FieldDescriptor* previous_field = nullptr;
  for (absl::string_view path_element : elements) {
    ASSIGN_OR_RETURN((auto [field_name, access_info]),
                     ParseProtopathElement(path_element));
    const google::protobuf::Descriptor* current_descr;
    if (previous_field != nullptr) {
      current_descr = previous_field->message_type();
      if (current_descr == nullptr) {
        return absl::FailedPreconditionError(absl::StrFormat(
            "unexpected type of the field `%s` in the protopath "
            "`%s`: expected a message",
            previous_field->name(), protopath));
      }
    } else {
      current_descr = descr;
    }
    const google::protobuf::FieldDescriptor* field_descriptor =
        current_descr->FindFieldByName(field_name);
    if (field_descriptor == nullptr) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "unknown field `%s` in the message `%s` in the protopath `%s`.",
          field_name, current_descr->full_name(), protopath));
    }
    if (field_descriptor->enum_type() != nullptr ||
        field_descriptor->is_extension()) {
      return absl::FailedPreconditionError(absl::StrFormat(
          "unsupported type `%s` of the field `%s` in the protopath `%s`.",
          field_descriptor->type_name(), field_descriptor->name(), protopath));
    }
    if (field_descriptor->is_repeated() &&
        std::holds_alternative<proto::RegularFieldAccess>(access_info)) {
      access_info = proto::RepeatedFieldAccess{};
    }
    fields.push_back(field_descriptor);
    access_infos.push_back(access_info);
    previous_field = field_descriptor;
  }
  bool is_size_protopath =
      std::holds_alternative<proto::RepeatedFieldSizeAccess>(
          access_infos.back());
  if (previous_field->message_type() != nullptr && !is_size_protopath) {
    return absl::FailedPreconditionError(absl::StrCat(
        "unexpected type of the last field in protopath `%s`", protopath));
  }
  return CreateReaderWithStringType(fields, std::move(access_infos),
                                    string_type);
}

}  // namespace

ProtoFieldsLoader::ProtoFieldsLoader(ProtoFieldsLoader::PrivateConstructorTag,
                                     const google::protobuf::Descriptor* descr,
                                     proto::StringFieldType string_type)
    : descr_(descr), string_type_(string_type) {}

absl::StatusOr<std::unique_ptr<InputLoader<google::protobuf::Message>>>
ProtoFieldsLoader::Create(const google::protobuf::Descriptor* descr,
                          proto::StringFieldType string_type) {
  return std::make_unique<ProtoFieldsLoader>(PrivateConstructorTag{}, descr,
                                             string_type);
}

absl::Nullable<const QType*> ProtoFieldsLoader::GetQTypeOf(
    absl::string_view name) const {
  ASSIGN_OR_RETURN(const auto& reader,
                   ParseProtopathToReader(descr_, name, string_type_), nullptr);
  return reader->qtype();
}

std::vector<std::string> ProtoFieldsLoader::SuggestAvailableNames() const {
  return {};
}

absl::StatusOr<BoundInputLoader<google::protobuf::Message>> ProtoFieldsLoader::BindImpl(
    const absl::flat_hash_map<std::string, TypedSlot>& output_slots) const {
  std::vector<ProtoTypeReader::BoundReadFn> readers;

  for (const auto& [name, slot] : output_slots) {
    ASSIGN_OR_RETURN(const auto& reader,
                     ParseProtopathToReader(descr_, name, string_type_));
    if (reader->qtype() != slot.GetType()) {
      return absl::FailedPreconditionError(
          absl::StrFormat("invalid type for slot %s: expected %s, got %s", name,
                          slot.GetType()->name(), reader->qtype()->name()));
    }
    // TODO: combine readers with the same QType.
    ASSIGN_OR_RETURN(auto read_fn, reader->BindReadFn(slot));
    readers.push_back(read_fn);
  }
  // Load data from provided input into the frame.
  return BoundInputLoader<google::protobuf::Message>(
      [descr_(this->descr_), readers_(std::move(readers))](
          const google::protobuf::Message& m, FramePtr frame,
          RawBufferFactory*) -> absl::Status {
        if (descr_ != m.GetDescriptor()) {
          return absl::FailedPreconditionError(
              "message must have the same descriptor as provided during "
              "construction of ProtoFieldsLoader");
        }
        for (const auto& r : readers_) {
          r(m, frame);
        }
        return absl::OkStatus();
      });
}

}  // namespace arolla
