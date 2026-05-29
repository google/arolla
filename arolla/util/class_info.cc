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
#include "arolla/util/class_info.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory_resource>
#include <ostream>
#include <string>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/util/api.h"
#include "arolla/util/demangle.h"

namespace arolla {
namespace {

using ClassId = uint32_t;

// NOTE: Soft limits for monitoring usage. Safe to increase; no underlying logic
// depends on these specific values.
constexpr size_t kMaxDepth = 8;
constexpr size_t kMaxId = 32767;

class ClassRegistry {
 public:
  struct Record {
    const std::type_info* type = nullptr;

    // NOTE: Use std::pmr::vector with a monotonic allocator to improve
    // memory locality for different lineages.
    std::pmr::vector<ClassId> lineage;
  };

  static ClassRegistry& Instance() {
    static absl::NoDestructor<ClassRegistry> instance;
    return *instance;
  }

  ClassRegistry() : records_(/*size=*/1) {}

  ClassInfo RegisterClass(const std::type_info& type,
                          absl::Span<const ClassId> ancestors) {
    absl::MutexLock guard(lock_);
    auto& id = type_to_id_[type];
    if (id == 0) {
      if (records_.size() > kMaxId) {
        LOG(FATAL) << "too many dynamic classes registered (limit: " << kMaxId
                   << ")";
      }
      if (ancestors.size() > kMaxDepth) {
        LOG(FATAL) << "inheritance depth overflow for class " << TypeName(type)
                   << " (limit: " << static_cast<int>(kMaxDepth) << ")";
      }
      id = static_cast<ClassId>(records_.size());
      std::pmr::vector<ClassId> lineage(&memory_resource_);
      lineage.reserve(ancestors.size() + 2);
      lineage.push_back(ancestors.size() + 1);
      lineage.insert(lineage.end(), ancestors.begin(), ancestors.end());
      lineage.push_back(id);
      records_.emplace_back(
          Record{.type = &type, .lineage = std::move(lineage)});
    } else if (ancestors.empty()) {
      if (records_[id].lineage.size() > 2) {
        LOG(FATAL) << "class " << TypeName(type) << " is already registered as"
                   << " a subclass, cannot be re-registered as a root class";
      }
    } else {
      auto parent_id = ancestors.back();
      auto& record = records_[id];
      if (record.lineage.size() <= 2) {
        LOG(FATAL)
            << "class " << TypeName(type) << " is already registered as"
            << " a root class, and cannot be re-registered as a subclass of "
            << TypeName(*records_[parent_id].type);
      }
      if (record.lineage.size() != ancestors.size() + 2 ||
          record.lineage.rbegin()[1] != parent_id) {
        LOG(FATAL) << "class " << TypeName(type)
                   << " is already registered as a subclass of "
                   << TypeName(*records_[record.lineage.rbegin()[1]].type)
                   << ", and cannot be re-registered as a subclass of "
                   << TypeName(*records_[parent_id].type);
      }
    }
    return ClassInfo::unsafe_from_lineage(ClassInfo::Lineage{
        .raw_lineage = &records_[id].lineage[0],
    });
  }

  std::string GetNameOf(ClassId id) {
    const std::type_info* type;
    {
      absl::MutexLock guard(lock_);
      type = records_[id].type;
    }
    return TypeName(*type);
  }

 private:
  absl::Mutex lock_;
  absl::flat_hash_map<std::type_index, ClassId> type_to_id_
      ABSL_GUARDED_BY(lock_);
  std::deque<Record> records_ ABSL_GUARDED_BY(lock_);
  std::pmr::monotonic_buffer_resource memory_resource_ ABSL_GUARDED_BY(lock_){
      1024, std::pmr::new_delete_resource()};
};

}  // namespace

std::string ClassInfo::name() const {
  return ClassRegistry::Instance().GetNameOf(lineage_[lineage_[0]]);
}

std::string ClassInfo::stringify() const {
  return absl::StrCat("ClassInfo{'", absl::Utf8SafeCHexEscape(name()), "'}");
}

AROLLA_API ClassInfo RegisterRootClass(const std::type_info& type) {
  return ClassRegistry::Instance().RegisterClass(type, /*ancestors=*/{});
}

AROLLA_API ClassInfo RegisterSubclass(const std::type_info& type,
                                      ClassInfo parent_info) {
  auto lineage = parent_info.unsafe_lineage();
  return ClassRegistry::Instance().RegisterClass(
      type, absl::Span<const ClassId>(lineage.raw_lineage + 1,
                                      lineage.raw_lineage[0]));
}

std::ostream& operator<<(std::ostream& os, ClassInfo class_info) {
  return os << "ClassInfo{'" << absl::Utf8SafeCHexEscape(class_info.name())
            << "'}";
}

}  // namespace arolla
