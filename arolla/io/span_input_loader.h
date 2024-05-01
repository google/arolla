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
#ifndef AROLLA_IO_SPAN_INPUT_LOADER_H_
#define AROLLA_IO_SPAN_INPUT_LOADER_H_

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "arolla/io/input_loader.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {
namespace span_input_loader_impl {

template <typename T>
struct InputTraits {
  using ArollaType = T;
};
template <typename T>
struct InputTraits<std::optional<T>> {
  using ArollaType = OptionalValue<T>;
};

template <typename T>
std::vector<std::pair<std::string, QTypePtr>> MakeQTypesList(
    std::vector<std::string> names_in_order) {
  std::vector<std::pair<std::string, QTypePtr>> qtypes;
  qtypes.reserve(names_in_order.size());
  for (const auto& name : names_in_order) {
    qtypes.emplace_back(name, GetQType<T>());
  }
  return qtypes;
}

}  // namespace span_input_loader_impl

// InputLoader implementation for Span elements.
template <typename T>
class SpanInputLoader : public StaticInputLoader<absl::Span<const T>> {
  using Input = absl::Span<const T>;
  using ArollaT = typename span_input_loader_impl::InputTraits<T>::ArollaType;

 public:
  // Creates SpanInputLoader that accepts a span of output_names_in_order.size()
  // elements and loads them under output_names_in_order names correspondingly.
  //
  // All the Arolla types are copied into the frame as is, std::optional is
  // automatically converted into arolla::OptionalValue.
  //
  static InputLoaderPtr<absl::Span<const T>> Create(
      std::vector<std::string> output_names_in_order) {
    // Not using make_shared to avoid binary size blowup.
    return InputLoaderPtr<absl::Span<const T>>(static_cast<InputLoader<Input>*>(
        new SpanInputLoader<T>(std::move(output_names_in_order))));
  }

 private:
  explicit SpanInputLoader(std::vector<std::string> output_names_in_order)
      : StaticInputLoader<absl::Span<const T>>(
            span_input_loader_impl::MakeQTypesList<ArollaT>(
                output_names_in_order)) {}

  absl::StatusOr<BoundInputLoader<Input>> BindImpl(
      const absl::flat_hash_map<std::string, TypedSlot>& output_slots)
      const override {
    std::vector<size_t> element_ids;
    std::vector<FrameLayout::Slot<ArollaT>> slots;

    for (size_t i = 0; i != this->types_in_order().size(); ++i) {
      if (auto it = output_slots.find(this->types_in_order()[i].first);
          it != output_slots.end()) {
        ASSIGN_OR_RETURN(auto slot, it->second.template ToSlot<ArollaT>());
        element_ids.push_back(i);
        slots.push_back(slot);
      }
    }
    return BoundInputLoader<Input>(
        [slots = std::move(slots), element_ids = std::move(element_ids),
         expected_input_size = this->types_in_order().size()](
            const Input& input, FramePtr frame,
            RawBufferFactory*) -> absl::Status {
          if (input.size() != expected_input_size) {
            return absl::InvalidArgumentError(
                absl::StrFormat("unexpected input count: expected %d, got %d",
                                expected_input_size, input.size()));
          }
          for (size_t i = 0; i < slots.size(); ++i) {
            size_t id = element_ids[i];
            DCHECK_LT(id, input.size());
            frame.Set(slots[i], ArollaT(input[id]));
          }
          return absl::OkStatus();
        });
  }
};

}  // namespace arolla

#endif  // AROLLA_IO_SPAN_INPUT_LOADER_H_
