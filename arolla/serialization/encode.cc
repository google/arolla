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

#include <string>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/serialization_base/container.h"
#include "arolla/serialization_base/container_proto.h"
#include "arolla/serialization_base/encode.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::ContainerProto;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueEncoder;
using ::arolla::serialization_base::ValueProto;

// The global registry of value decoders.
class ValueEncoderRegistry {
 public:
  static ValueEncoderRegistry& instance() {
    static Indestructible<ValueEncoderRegistry> result;
    return *result;
  }

  absl::Status RegisterValueEncoderByQType(QTypePtr qtype,
                                           ValueEncoder value_encoder) {
    if (qtype == nullptr) {
      return absl::InvalidArgumentError("qtype is null");
    }
    if (value_encoder == nullptr) {
      return absl::InvalidArgumentError("value_encoder is empty");
    }
    absl::MutexLock lock(&mutex_);
    auto& item = qtype_based_registry_[qtype];
    if (item != nullptr) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value_encoder for qtype=%s has been already registered",
          qtype->name()));
    }
    item = std::move(value_encoder);
    return absl::OkStatus();
  }

  absl::Status RegisterValueEncoderByQValueSpecialisationKey(
      absl::string_view key, ValueEncoder value_encoder) {
    if (key.empty()) {
      return absl::InvalidArgumentError("key is empty");
    }
    if (value_encoder == nullptr) {
      return absl::InvalidArgumentError("value_encoder is empty");
    }
    absl::MutexLock lock(&mutex_);
    auto& record = key_based_registry_[key];
    if (record != nullptr) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "value_encoder for key='%s' has been already registered",
          absl::Utf8SafeCHexEscape(key)));
    }
    record = std::move(value_encoder);
    return absl::OkStatus();
  }

  absl::StatusOr<ValueProto> EncodeValue(TypedRef value, Encoder& encoder) {
    ValueEncoder value_encoder;
    if (value.GetType() == GetQType<QTypePtr>()) {
      ASSIGN_OR_RETURN(value_encoder,
                       FindValueEncoderForQType(value.UnsafeAs<QTypePtr>()));
    } else {
      ASSIGN_OR_RETURN(value_encoder, FindValueEncoderForNonQType(value));
    }
    return value_encoder(value, encoder);
  }

 private:
  absl::StatusOr<ValueEncoder> FindValueEncoderForQType(QTypePtr qtype) {
    const auto& qtype_key = qtype->qtype_specialization_key();
    {  // NOTE: Consider using ReaderLock if there is read-access congestion.
      absl::MutexLock lock(&mutex_);
      if (auto it = qtype_based_registry_.find(qtype);  // (q0)
          it != qtype_based_registry_.end()) {
        return it->second;
      }
      if (!qtype_key.empty()) {
        if (auto it = key_based_registry_.find(qtype_key);  // (q1)
            it != key_based_registry_.end()) {
          return it->second;
        }
      }
    }
    return absl::UnimplementedError(absl::StrFormat(
        "cannot serialize qtype=%s, specialization_key='%s'; this may indicate "
        "a missing BUILD dependency on the encoder for this qtype",
        qtype->name(),
        absl::Utf8SafeCHexEscape(qtype->qtype_specialization_key())));
  }

  absl::StatusOr<ValueEncoder> FindValueEncoderForNonQType(TypedRef value) {
    DCHECK(value.GetType() != GetQType<QTypePtr>());
    const auto& qvalue_key = value.PyQValueSpecializationKey();
    const auto& qtype = value.GetType();
    const auto& qtype_key = qtype->qtype_specialization_key();
    {  // NOTE: Consider using ReaderLock if there is read-access congestion.
      absl::MutexLock lock(&mutex_);
      if (!qvalue_key.empty()) {  // (p0)
        if (auto it = key_based_registry_.find(qvalue_key);
            it != key_based_registry_.end()) {
          return it->second;
        }
      }
      if (auto it = qtype_based_registry_.find(qtype);  // (p1)
          it != qtype_based_registry_.end()) {
        return it->second;
      }
      if (!qtype_key.empty()) {  // (p2)
        if (auto it = key_based_registry_.find(qtype_key);
            it != key_based_registry_.end()) {
          return it->second;
        }
      }
    }
    return absl::UnimplementedError(absl::StrFormat(
        "cannot serialize value: specialization_key='%s', qtype=%s, "
        "qtype_specialization_key='%s': %s; this may indicate a missing BUILD "
        "dependency on the encoder for this qtype",
        absl::Utf8SafeCHexEscape(qvalue_key), qtype->name(),
        absl::Utf8SafeCHexEscape(qtype_key), value.Repr()));
  }

  absl::Mutex mutex_;
  absl::flat_hash_map<std::string, ValueEncoder> key_based_registry_
      ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<QTypePtr, ValueEncoder> qtype_based_registry_
      ABSL_GUARDED_BY(mutex_);
};

absl::Status EncodeToContainerBuilder(
    absl::Span<const TypedValue> values, absl::Span<const ExprNodePtr> exprs,
    arolla::serialization_base::ContainerBuilder& container_builder) {
  arolla::serialization_base::Encoder encoder(
      [](TypedRef value, Encoder& encoder) {
        return ValueEncoderRegistry::instance().EncodeValue(value, encoder);
      },
      container_builder);
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
  arolla::serialization_base::ContainerProtoBuilder result;
  RETURN_IF_ERROR(EncodeToContainerBuilder(values, exprs, result));
  return std::move(result).Finish();
}

absl::Status RegisterValueEncoderByQType(QTypePtr qtype,
                                         ValueEncoder value_encoder) {
  return ValueEncoderRegistry::instance().RegisterValueEncoderByQType(
      qtype, value_encoder);
}

absl::Status RegisterValueEncoderByQValueSpecialisationKey(
    absl::string_view key, ValueEncoder value_encoder) {
  return ValueEncoderRegistry::instance()
      .RegisterValueEncoderByQValueSpecialisationKey(key, value_encoder);
}

}  // namespace arolla::serialization
