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
#include "arolla/sequence/mutable_sequence.h"

#include <cstddef>
#include <memory>

#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/util/memory.h"

namespace arolla {

absl::StatusOr<MutableSequence> MutableSequence::Make(QTypePtr value_qtype,
                                                      size_t size) {
  DCHECK_NE(value_qtype, nullptr);
  DCHECK_GE(size, 0);
  MutableSequence result;
  result.value_qtype_ = value_qtype;
  if (size <= 0) {
    return result;
  }
  result.size_ = size;
  const auto& element_layout = value_qtype->type_layout();
  const auto total_byte_size = element_layout.AllocSize() * size;
  auto memory = AlignedAlloc(element_layout.AllocAlignment(), total_byte_size);
  if (memory == nullptr) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "AlignedAlloc has failed: alignment=%d, total_size=%d",
        element_layout.AllocAlignment().value, total_byte_size));
  }
  element_layout.InitializeAlignedAllocN(memory.get(), size);
  auto memory_deleter = memory.get_deleter();
  auto* memory_ptr = memory.release();
  result.data_ = std::shared_ptr<void>(
      memory_ptr,
      [value_qtype, size, memory_deleter = memory_deleter](void* ptr) {
        value_qtype->type_layout().DestroyAllocN(ptr, size);
        memory_deleter(ptr);
      });
  return result;
}

}  // namespace arolla
