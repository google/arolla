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
#include "arolla/qtype/optional_qtype.h"

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/unit.h"

namespace arolla {
namespace {

class OptionalQTypeMaps {
 public:
  // Register a qtype, optional_qtype pair.
  void Register(QTypePtr qtype, QTypePtr optional_qtype) {
    absl::MutexLock l(&lock_);
    to_optional_[qtype] = optional_qtype;
    to_optional_[optional_qtype] = optional_qtype;
  }

  // Given a qtype, return corresponding optional qtype if it exists.
  absl::StatusOr<QTypePtr> ToOptionalQType(QTypePtr qtype) {
    absl::ReaderMutexLock l(&lock_);
    auto iter = to_optional_.find(qtype);
    if (iter == to_optional_.end()) {
      return absl::InvalidArgumentError(
          absl::StrCat("no optional qtype for ", qtype->name()));
    }
    return iter->second;
  }

  // Check whether the given QType is an optional QType.
  bool IsOptionalQType(QTypePtr qtype) {
    absl::ReaderMutexLock l(&lock_);
    auto iter = to_optional_.find(qtype);
    return iter != to_optional_.end() && iter->second == qtype;
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<QTypePtr, QTypePtr> to_optional_ ABSL_GUARDED_BY(lock_);
};

OptionalQTypeMaps* GetOptionalQTypeMaps() {
  static absl::NoDestructor<OptionalQTypeMaps> instance;
  return instance.get();
}

}  // namespace

void RegisterOptionalQType(QTypePtr optional_qtype) {
  const auto* value_qtype = optional_qtype->value_qtype();
  DCHECK(value_qtype != nullptr);
  const auto& sub_slots = optional_qtype->type_fields();
  DCHECK_GT(sub_slots.size(), 0);
  DCHECK(sub_slots[0].GetType()->type_info() == typeid(bool));
  DCHECK_EQ(sub_slots[0].byte_offset(), 0);
  if (sub_slots.size() == 1) {
    DCHECK_EQ(value_qtype, GetQType<Unit>());
  } else if (sub_slots.size() == 2) {
    DCHECK_EQ(sub_slots[1].GetType(), value_qtype);
  } else {
    LOG(FATAL) << "Unexpected number of subslots in optional: "
                << sub_slots.size();
  }
  GetOptionalQTypeMaps()->Register(value_qtype, optional_qtype);
}

absl::StatusOr<QTypePtr> ToOptionalQType(QTypePtr qtype) {
  return GetOptionalQTypeMaps()->ToOptionalQType(qtype);
}

const QType* /*nullable*/ DecayOptionalQType(const QType* /*nullable*/ qtype) {
  return IsOptionalQType(qtype) ? qtype->value_qtype() : qtype;
}

bool IsOptionalQType(const QType* /*nullable*/ qtype) {
  // Use the properties we verified earlier to discard the non-optional types
  // before locking the mutex.
  return (qtype != nullptr && qtype->value_qtype() != nullptr &&
          !qtype->type_fields().empty() &&
          GetOptionalQTypeMaps()->IsOptionalQType(qtype));
}

absl::StatusOr<FrameLayout::Slot<bool>> GetPresenceSubslotFromOptional(
    TypedSlot slot) {
  if (!IsOptionalQType(slot.GetType())) {
    return absl::InvalidArgumentError(
        absl::StrCat("'", slot.GetType()->name(), "' is not optional qtype."));
  }
  if (slot.SubSlotCount() == 0) {
    return absl::InternalError("optional value has no subslots.");
  }
  return slot.SubSlot(0).ToSlot<bool>();
}

absl::StatusOr<TypedSlot> GetValueSubslotFromOptional(TypedSlot slot) {
  if (!IsOptionalQType(slot.GetType())) {
    return absl::InvalidArgumentError(
        absl::StrCat("'", slot.GetType()->name(), "' is not optional qtype."));
  }
  if (slot.SubSlotCount() != 2) {
    return absl::InvalidArgumentError(absl::StrCat(
        "'", slot.GetType()->name(), "' does not have a value subslot."));
  }
  return slot.SubSlot(1);
}

absl::StatusOr<TypedValue> CreateMissingValue(QTypePtr optional_qtype) {
  if (!IsOptionalQType(optional_qtype)) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "cannot create a missing value for non-optional qtype `%s`",
        optional_qtype->name()));
  }
  return TypedValue::UnsafeFromTypeDefaultConstructed(optional_qtype);
}

}  // namespace arolla
