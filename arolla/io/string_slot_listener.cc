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
#include "arolla/io/string_slot_listener.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/io/accessors_slot_listener.h"
#include "arolla/io/slot_listener.h"
#include "arolla/memory/optional_value.h"
#include "arolla/util/bytes.h"

namespace arolla {

absl::StatusOr<std::unique_ptr<SlotListener<std::string>>> BytesSlotListener(
    absl::string_view side_output_name) {
  return CreateAccessorsSlotListener<std::string>(
      side_output_name, [](const OptionalValue<Bytes>& b, std::string* out) {
        *out = b.present ? b.value : "";
      });
}

absl::StatusOr<std::unique_ptr<SlotListener<std::vector<std::string>>>>
BytesArraySlotListener(absl::string_view side_output_name) {
  return CreateAccessorsSlotListener<std::vector<std::string>>(
      side_output_name,
      [](const DenseArray<Bytes>& arr, std::vector<std::string>* out) {
        out->clear();
        out->reserve(arr.size());
        arr.ForEach([&](auto _, bool is_present, absl::string_view value) {
          out->push_back(is_present ? std::string(value) : "");
        });
      });
}

}  // namespace arolla
