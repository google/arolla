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
#include "arolla/serialization/encode.h"

#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/serialization_base/container_proto.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::ContainerBuilder;
using ::arolla::serialization_base::ContainerProto;
using ::arolla::serialization_base::ContainerProtoBuilder;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_codecs::CodecBasedValueEncoder;

// The global registry of value decoders.

absl::Status EncodeToContainerBuilder(absl::Span<const TypedValue> values,
                                      absl::Span<const ExprNodePtr> exprs,
                                      ContainerBuilder& container_builder) {
  Encoder encoder(CodecBasedValueEncoder(), container_builder);
  for (const auto& value : values) {
    RETURN_IF_ERROR(encoder.EncodeValue(value).status());
  }
  for (const auto& expr : exprs) {
    RETURN_IF_ERROR(encoder.EncodeExpr(expr).status());
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<ContainerProto> Encode(absl::Span<const TypedValue> values,
                                      absl::Span<const ExprNodePtr> exprs) {
  ContainerProtoBuilder result;
  RETURN_IF_ERROR(EncodeToContainerBuilder(values, exprs, result));
  return std::move(result).Finish();
}

}  // namespace arolla::serialization
