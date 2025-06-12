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
#include "arolla/qexpr/evaluation_engine.h"

#include <memory>
#include <optional>
#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

absl::StatusOr<std::unique_ptr<BoundExpr>> InplaceCompiledExpr::Bind(
    FrameLayout::Builder* layout_builder,
    const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
    std::optional<TypedSlot> output_slot) const {
  if (!output_slot.has_value()) {
    output_slot = AddSlot(output_type(), layout_builder);
  }
  return InplaceBind(input_slots, *output_slot,
                     AddSlotsMap(named_output_types(), layout_builder));
}

}  // namespace arolla
