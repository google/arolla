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
#include "arolla/qtype/typed_ref.h"

#include "absl//status/status.h"
#include "absl//strings/str_format.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

TypedRef TypedRef::FromSlot(TypedSlot slot, ConstFramePtr ptr) {
  ptr.DCheckFieldType(slot.byte_offset(), slot.GetType()->type_info());
  return TypedRef(slot.GetType(), ptr.GetRawPointer(slot.byte_offset()));
}

absl::Status TypedRef::CopyToSlot(TypedSlot slot, FramePtr frame) const {
  if (type_ != slot.GetType()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("slot type does not match: expected %s, got %s",
                        type_->name(), slot.GetType()->name()));
  }
  type_->UnsafeCopy(value_ptr_, frame.GetRawPointer(slot.byte_offset()));
  return absl::OkStatus();
}

}  // namespace arolla
