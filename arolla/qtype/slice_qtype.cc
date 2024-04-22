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
#include "arolla/qtype/slice_qtype.h"

#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/tuple_qtype.h"
#include "arolla/util/fast_dynamic_downcast_final.h"
#include "arolla/util/indestructible.h"
#include "arolla/util/repr.h"

namespace arolla {
namespace {

// Returns name of the SliceQType from constructor arguments.
std::string SliceQTypeName(QTypePtr start, QTypePtr stop, QTypePtr step) {
  return absl::StrCat("slice<", JoinTypeNames({start, stop, step}), ">");
}

class SliceQType final : public BasicDerivedQType {
 public:
  SliceQType(QTypePtr start, QTypePtr stop, QTypePtr step)
      : BasicDerivedQType(ConstructorArgs{
            .name = SliceQTypeName(start, stop, step),
            .base_qtype = MakeTupleQType({start, stop, step}),
            .qtype_specialization_key =
                std::string(GetSliceQTypeSpecializationKey()),
        }) {}

  ReprToken UnsafeReprToken(const void* source) const override {
    return ReprToken{
        absl::StrCat("slice", GetBaseQType()->UnsafeReprToken(source).str)};
  }
};

// Registry of SliceQTypes that provides a guarantee that each qtype is a
// singleton.
class SliceQTypeRegistry {
 public:
  static SliceQTypeRegistry* instance() {
    static Indestructible<SliceQTypeRegistry> result;
    return result.get();
  }

  QTypePtr GetQType(QTypePtr start, QTypePtr stop, QTypePtr step)
      ABSL_LOCKS_EXCLUDED(lock_) {
    {  // Fast look-up without memory allocation.
      absl::ReaderMutexLock guard(&lock_);
      if (const auto it = registry_.find({start, stop, step});
          it != registry_.end()) {
        return it->second.get();
      }
    }
    auto slice_qtype = std::make_unique<SliceQType>(start, stop, step);
    absl::MutexLock guard(&lock_);
    return registry_.try_emplace({start, stop, step}, std::move(slice_qtype))
        .first->second.get();
  }

 private:
  using RegistryKey = std::tuple<QTypePtr, QTypePtr, QTypePtr>;

  absl::Mutex lock_;
  absl::flat_hash_map<RegistryKey, std::unique_ptr<SliceQType>> registry_
      ABSL_GUARDED_BY(lock_);
};

}  // namespace

bool IsSliceQType(const QType* /*nullable*/ qtype) {
  return fast_dynamic_downcast_final<const SliceQType*>(qtype) != nullptr;
}

QTypePtr MakeSliceQType(QTypePtr start, QTypePtr stop, QTypePtr step) {
  return SliceQTypeRegistry::instance()->GetQType(start, stop, step);
}

absl::string_view GetSliceQTypeSpecializationKey() {
  return "::arolla::SliceQType";
}

}  // namespace arolla
