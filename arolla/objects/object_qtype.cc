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
#include "arolla/objects/object_qtype.h"

#include <algorithm>
#include <optional>
#include <sstream>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/base/nullability.h"
#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/refcount_ptr.h"
#include "arolla/util/repr.h"
#include "arolla/util/string.h"

namespace arolla {

namespace {


}  // namespace

const Object::Impl& Object::Impl::Empty() {
  static absl::NoDestructor<Impl> impl;
  return *impl;
}

const Object::Attributes& Object::attributes() const {
  return impl_ != nullptr ? impl_->attributes : Impl::Empty().attributes;
}

const std::optional<Object>& Object::prototype() const {
  return impl_ != nullptr ? impl_->prototype : Impl::Empty().prototype;
}

const TypedValue* absl_nullable Object::GetAttrOrNull(
    absl::string_view attr) const {
  auto* impl = impl_.get();
  while (impl != nullptr) {
    if (auto it = impl->attributes.find(attr); it != impl->attributes.end()) {
      return &it->second;
    }
    impl = impl->prototype.has_value() ? impl->prototype->impl_.get() : nullptr;
  }
  return nullptr;
}

std::vector<Object::Attributes::const_pointer> Object::GetSortedAttributes()
    const {
  const auto& attrs = attributes();
  std::vector<Object::Attributes::const_pointer> items;
  items.reserve(attrs.size());
  for (const auto& pair : attrs) {
    items.push_back(&pair);
  }
  std::sort(items.begin(), items.end(),
            [](const auto& a, const auto& b) { return a->first < b->first; });
  return items;
}

void Object::ArollaFingerprint(FingerprintHasher* hasher) const {
  hasher->Combine(absl::string_view("::arolla::Object"));
  for (const auto* pair : GetSortedAttributes()) {
    hasher->Combine(pair->first);
    hasher->Combine(pair->second.GetFingerprint());
  }
  if (prototype().has_value()) {
    hasher->Combine(*prototype());
  }
}

ReprToken Object::ArollaReprToken() const {
  std::ostringstream result;
  result << "Object{attributes={";
  bool first = true;
  for (const auto* pair : GetSortedAttributes()) {
    result << NonFirstComma(first) << absl::Utf8SafeCHexEscape(pair->first)
           << "=" << pair->second.Repr();
  }
  result << "}";
  if (const auto& proto = prototype(); proto.has_value()) {
    result << ", prototype=" << Repr(*proto);
  }
  result << "}";
  return ReprToken{std::move(result).str()};
}

AROLLA_DEFINE_SIMPLE_QTYPE(OBJECT, Object);

}  // namespace arolla
