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
#ifndef AROLLA_QTYPE_DICT_DICT_TYPES_H_
#define AROLLA_QTYPE_DICT_DICT_TYPES_H_

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/util/bytes.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/map.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/text.h"
#include "arolla/util/unit.h"
#include "arolla/util/view_types.h"

namespace arolla {

// Returns true if `qtype` is a dict qtype.
bool IsDictQType(const QType* /*nullable*/ qtype);

// KeyToRowDict from Key to row id.
template <typename Key>
class KeyToRowDict {
  // We are using default hasher and equality from flat_hash_map with view type
  // as key. We assume that Key has implicit and cheap conversion to a view.
  // Comparison and hash computation will be performed on a view type.
  // For ::arolla::Text and ::arolla::Bytes we will have additional benefit of
  // heterogeneous lookup by any type convertible to string_view.
  using KeyViewHashTable = absl::flat_hash_map<view_type_t<Key>, int64_t>;
  using Hasher = typename KeyViewHashTable::hasher;
  using KeyEqual = typename KeyViewHashTable::key_equal;

 public:
  using Map = absl::flat_hash_map<Key, int64_t, Hasher, KeyEqual>;

  KeyToRowDict() = default;
  explicit KeyToRowDict(Map dict)
      : dict_(std::make_shared<Map>(std::move(dict))) {}

  KeyToRowDict(std::initializer_list<typename Map::value_type> dict)
      : dict_(std::make_shared<Map>(std::move(dict))) {}

  // Returns underlying data or empty map if not initialized.
  const Map& map() const {
    static const absl::NoDestructor<Map> empty;
    return dict_ != nullptr ? *dict_ : *empty;
  }

 private:
  // shared_ptr in order to perform fast copying
  std::shared_ptr<const Map> dict_;
};

namespace dict_impl {
// Registers a dict QType with the given key type. Fails (in debug build) in
// case if the dict is already registered.
void RegisterKeyToRowDictQType(QTypePtr key_type, QTypePtr dict_type);
}  // namespace dict_impl

// Returns a dict QType with the given key type.
// For the sake of smaller binary size it is registered only at the first use of
// the dict QType. So the function returns "not found" if it was not used
// before the call.
absl::StatusOr<QTypePtr> GetKeyToRowDictQType(QTypePtr key_type);

// Returns a dict QType with the given key type.
template <typename Key>
QTypePtr GetKeyToRowDictQType() {
  return GetQType<KeyToRowDict<Key>>();
}

// Returns true is the given QType is KeyToRowDict QType.
bool IsKeyToRowDictQType(QTypePtr type);

// Returns a dict QType with the given key and value types.
absl::StatusOr<QTypePtr> GetDictQType(QTypePtr key_type, QTypePtr value_type);

// Returns key QType for the provided dict type or nullptr if the type is not
// dict.
const QType* /*nullable*/ GetDictKeyQTypeOrNull(QTypePtr dict_type);
// Returns value QType for the provided dict type or nullptr if the type is
// not dict.
const QType* /*nullable*/ GetDictValueQTypeOrNull(QTypePtr dict_type);

// Define QType for KeyToRowDict<Key>.
// NOTE: Key is a value_type for KeyToRowDict<Key>.
template <typename Key>
struct QTypeTraits<KeyToRowDict<Key>> {
  // Prohibit unexpected instantiations.
  static_assert(!std::is_same_v<Key, Unit>);
  static_assert(!std::is_same_v<Key, float>);
  static_assert(!std::is_same_v<Key, double>);
  static_assert(is_scalar_type_v<strip_optional_t<Key>>);

  static QTypePtr type() {
    static const absl::NoDestructor<SimpleQType> result(
        meta::type<KeyToRowDict<Key>>(),
        absl::StrCat("DICT_", GetQType<Key>()->name()),
        /*value_qtype=*/GetQType<Key>(),
        /*qtype_specialization_key=*/"::arolla::KeyToRowDict");
    static const int ABSL_ATTRIBUTE_UNUSED dict_registered =
        (dict_impl::RegisterKeyToRowDictQType(GetQType<Key>(), result.get()),
         1);
    return result.get();
  }
};

// Define KeyToRowDict string representation.
template <class Key>
struct ReprTraits<KeyToRowDict<Key>,
                  std::enable_if_t<std::is_invocable_v<ReprTraits<Key>, Key>>> {
  ReprToken operator()(const KeyToRowDict<Key>& dict) const {
    std::ostringstream oss;
    oss << "dict{";
    for (const auto& key : SortedMapKeys(dict.map())) {
      oss << Repr(Key(key)) << ":" << Repr(dict.map().at(key)) << ",";
    }
    oss << "}";
    return ReprToken{std::move(oss).str()};
  }
};

// Define KeyToRowDict fingerprint.
template <class Key>
struct FingerprintHasherTraits<KeyToRowDict<Key>> {
  void operator()(FingerprintHasher* hasher,
                  const KeyToRowDict<Key>& dict) const {
    std::vector<std::pair<view_type_t<Key>, int64_t>> elements(
        dict.map().begin(), dict.map().end());
    std::sort(elements.begin(), elements.end());
    hasher->Combine(elements.size());
    for (const auto& [k, v] : elements) {
      hasher->Combine(k, v);
    }
  }
};

// Explicitly instantiate dict QTypes to avoid including them into each
// translation unit and reduce linker inputs size.
extern template struct QTypeTraits<KeyToRowDict<bool>>;
extern template struct QTypeTraits<KeyToRowDict<int32_t>>;
extern template struct QTypeTraits<KeyToRowDict<int64_t>>;
extern template struct QTypeTraits<KeyToRowDict<Bytes>>;
extern template struct QTypeTraits<KeyToRowDict<Text>>;

}  // namespace arolla

#endif  // AROLLA_QTYPE_DICT_DICT_TYPES_H_
