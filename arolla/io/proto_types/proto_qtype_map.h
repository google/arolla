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
#ifndef AROLLA_IO_PROTO_TYPES_PROTO_QTYPE_MAP_H_
#define AROLLA_IO_PROTO_TYPES_PROTO_QTYPE_MAP_H_

#include "absl/status/statusor.h"
#include "google/protobuf/descriptor.h"
#include "arolla/qtype/qtype.h"

namespace arolla::proto {

absl::StatusOr<arolla::QTypePtr> ProtoFieldTypeToQType(
    google::protobuf::FieldDescriptor::Type field_type);

}  // namespace arolla::proto

#endif  // AROLLA_IO_PROTO_TYPES_PROTO_QTYPE_MAP_H_
