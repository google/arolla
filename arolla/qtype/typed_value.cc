// Copyright 2025 Google LLC
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
#include "arolla/qtype/typed_value.h"

#include <cstddef>
#include <memory>
#include <new>  // IWYU pragma: keep
#include <utility>

#include "absl/base/call_once.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/memory.h"

namespace arolla {
namespace {

template <typename TypedRef /* = TypedRef | TypedValue*/>
absl::Status CheckPreconditionsForInitCompound(
    QTypePtr compound_qtype, absl::Span<const TypedRef> field_refs) {
  const auto& field_slots = compound_qtype->type_fields();
  if (field_slots.size() != field_refs.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "expected %d values, got %d; compound_qtype=%s", field_slots.size(),
        field_refs.size(), compound_qtype->name()));
  }
  for (size_t i = 0; i < field_refs.size(); ++i) {
    if (field_refs[i].GetType() != field_slots[i].GetType()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected fields[%d]: %s, got %s; compound_qtype=%s", i,
          field_slots[i].GetType()->name(), field_refs[i].GetType()->name(),
          compound_qtype->name()));
    }
  }
  return absl::OkStatus();
}

template <typename TypedRef /* = TypedRef | TypedValue*/>
void InitCompound(QTypePtr compound_qtype,
                  absl::Span<const TypedRef> field_refs, void* destination) {
  compound_qtype->type_layout().InitializeAlignedAlloc(destination);
  const auto& field_slots = compound_qtype->type_fields();
  FramePtr frame(destination, &compound_qtype->type_layout());
  for (size_t i = 0; i < field_refs.size(); ++i) {
    const auto& field_ref = field_refs[i];
    field_ref.GetType()->UnsafeCopy(
        field_ref.GetRawPointer(),
        frame.GetRawPointer(field_slots[i].byte_offset()));
  }
}

}  // namespace

TypedValue::Impl* TypedValue::AllocRawImpl(QTypePtr qtype) {
  const auto& type_layout = qtype->type_layout();
  const size_t alignment = type_layout.AllocAlignment().value;
  size_t extra_space = type_layout.AllocSize() + alignment;
  void* buffer = ::operator new(sizeof(Impl) + extra_space);
  Impl* impl = new (buffer) Impl;
  impl->qtype = qtype;
  impl->data = static_cast<char*>(buffer) + sizeof(Impl);
  void* tmp = std::align(alignment, extra_space, impl->data, extra_space);
  DCHECK_NE(tmp, nullptr);
  return impl;
}

TypedValue::Impl* TypedValue::AllocImpl(QTypePtr qtype, const void* value) {
  auto* impl = AllocRawImpl(qtype);
  qtype->type_layout().InitializeAlignedAlloc(impl->data);
  qtype->UnsafeCopy(value, impl->data);
  return impl;
}

TypedValue TypedValue::UnsafeFromTypeDefaultConstructed(QTypePtr qtype) {
  auto* impl = AllocRawImpl(qtype);
  qtype->type_layout().InitializeAlignedAlloc(impl->data);
  return TypedValue(impl);
}

absl::StatusOr<TypedValue> TypedValue::FromFields(
    QTypePtr compound_qtype, absl::Span<const TypedRef> fields) {
  if (auto status = CheckPreconditionsForInitCompound(compound_qtype, fields);
      !status.ok()) {
    return status;
  }
  auto* impl = AllocRawImpl(compound_qtype);
  InitCompound(compound_qtype, fields, impl->data);
  return TypedValue(impl);
}

absl::StatusOr<TypedValue> TypedValue::FromFields(
    QTypePtr compound_qtype, absl::Span<const TypedValue> fields) {
  if (auto status = CheckPreconditionsForInitCompound(compound_qtype, fields);
      !status.ok()) {
    return status;
  }
  auto* impl = AllocRawImpl(compound_qtype);
  InitCompound(compound_qtype, fields, impl->data);
  return TypedValue(impl);
}

const Fingerprint& TypedValue::GetFingerprint() const {
  // The fingerprint computation is expensive.
  // We compute it only on-demand, and do cache the result.
  absl::call_once(impl_->fingerprint_once, [impl = impl_] {
    FingerprintHasher hasher("TypedValue");
    hasher.Combine(impl->qtype);
    impl->qtype->UnsafeCombineToFingerprintHasher(impl->data, &hasher);
    impl->fingerprint = std::move(hasher).Finish();
  });
  return impl_->fingerprint;
}

}  // namespace arolla
