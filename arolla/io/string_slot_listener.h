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
#ifndef AROLLA_IO_STRING_SLOT_LISTENER_H_
#define AROLLA_IO_STRING_SLOT_LISTENER_H_

#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/io/slot_listener.h"

namespace arolla {

// Constructs a SlotListener that listens just to one side output called
// `side_output_name` of type arolla::Bytes and writes its contents into the
// provided std::string*.
//
// Usage example, listening for a "debug_html" side output:
//
// ASSIGN_OR_RETURN(
//   std::function<MyOutput(const MyInput&, std::string* /*side_output*/)>
//       model,
//   (arolla::ExprCompiler<MyInput, MyOutput, /*side_output=*/std::string>()
//     ... // set InputLoader and other options
//     .SetSlotListener(BytesSlotListener("debug_html"))
//     .Compile(my_model)));
//
absl::StatusOr<std::unique_ptr<SlotListener<std::string>>> BytesSlotListener(
    absl::string_view side_output_name);

// Constructs a SlotListener that listens just to one side output called
// `side_output_name` of type arolla::DenseArray<arolla::Bytes> and writes its
// contents into the provided std::vector<std::string>*. Missing fields will be
// represented by empty strings.
//
// Usage example, listening for a "debug_htmls" side output:
//
// ASSIGN_OR_RETURN(
//   std::function<MyOutput(const MyInput&, std::vector<std::string>*
//   /*side_output*/)>
//       model,
//   (arolla::ExprCompiler<MyInput, MyOutput, /*side_output=*/std::string>()
//     ... // set InputLoader and other options
//     .SetSlotListener(BytesArraySlotListener("debug_htmls"))
//     .Compile(my_model)));
//
absl::StatusOr<std::unique_ptr<SlotListener<std::vector<std::string>>>>
BytesArraySlotListener(absl::string_view side_output_name);

}  // namespace arolla

#endif  // AROLLA_IO_STRING_SLOT_LISTENER_H_
