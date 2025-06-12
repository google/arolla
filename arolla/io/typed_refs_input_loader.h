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
#ifndef AROLLA_IO_TYPED_VALUES_INPUT_LOADER_H_
#define AROLLA_IO_TYPED_VALUES_INPUT_LOADER_H_

#include <string>
#include <utility>
#include <vector>

#include "absl/types/span.h"
#include "arolla/io/input_loader.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"

namespace arolla {

// InputLoader for Span<const TypedRef>. The size of the span must be equal to
// the size of `args`. If InputLoader is bound partially (no slots for some of
// the inputs), it still expects Span with all inputs, but the non-requested
// inputs will not be copied to the context.
InputLoaderPtr<absl::Span<const TypedRef>> CreateTypedRefsInputLoader(
    const std::vector<std::pair<std::string, QTypePtr>>& args);

}  // namespace arolla

#endif  // AROLLA_IO_TYPED_VALUES_INPUT_LOADER_H_
