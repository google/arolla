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
#include "arolla/qexpr/qexpr_operator_signature.h"

#include <utility>
#include <vector>

#include "absl/base/const_init.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

QExprOperatorSignature::QExprOperatorSignature(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type)
    : input_types_(input_types.begin(), input_types.end()),
      output_type_(output_type) {}

const QExprOperatorSignature* QExprOperatorSignature::Get(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type) {
  using FnOperatorRegistryKey = std::pair<absl::Span<const QTypePtr>, QTypePtr>;
  static absl::Mutex lock(absl::kConstInit);
  static absl::NoDestructor<
      absl::flat_hash_map<FnOperatorRegistryKey, const QExprOperatorSignature*>>
      index;
  absl::MutexLock guard(&lock);
  auto it = index->find(FnOperatorRegistryKey(input_types, output_type));
  if (it != index->end()) {
    return it->second;
  }
  auto* result = new QExprOperatorSignature(input_types, output_type);
  (*index)[FnOperatorRegistryKey(result->input_types(),
                                 result->output_type())] = result;
  return result;
}

}  // namespace arolla
