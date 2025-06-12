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
#include "arolla/array/id_filter.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "arolla/memory/buffer.h"
#include "arolla/memory/raw_buffer_factory.h"
#include "arolla/util/fingerprint.h"

namespace arolla {

IdFilter IdFilter::UpperBoundMergeImpl(int64_t size,
                                       RawBufferFactory* buf_factory,
                                       const IdFilter& a, const IdFilter& b) {
  if (a.type() == kEmpty || b.type() == kFull) return b;
  if (b.type() == kEmpty || a.type() == kFull) return a;
  if (a.IsSame(b)) return a;

  if (std::max(a.ids().size(), b.ids().size()) >= size * kDenseSparsityLimit) {
    // For performance reason we switch from sparse to dense case if data is
    // not very sparse (at least one of the arguments has >=25% values present).
    return kFull;
  }

  Buffer<int64_t>::Builder bldr(a.ids().size() + b.ids().size(), buf_factory);
  auto inserter = bldr.GetInserter();
  auto ia = a.ids().begin();
  auto ib = b.ids().begin();

  while (ia != a.ids().end() && ib != b.ids().end()) {
    int64_t va = *ia - a.ids_offset();
    int64_t vb = *ib - b.ids_offset();
    int64_t v = std::min(va, vb);
    if (va == v) ia++;
    if (vb == v) ib++;
    inserter.Add(v);
  }
  while (ia != a.ids().end()) inserter.Add(*(ia++) - a.ids_offset());
  while (ib != b.ids().end()) inserter.Add(*(ib++) - b.ids_offset());

  return IdFilter(size, std::move(bldr).Build(inserter));
}

void FingerprintHasherTraits<IdFilter>::operator()(
    FingerprintHasher* hasher, const IdFilter& value) const {
  hasher->Combine(value.type());
  if (value.type() != IdFilter::Type::kFull) {
    hasher->Combine(value.ids());
  }
}

}  // namespace arolla
