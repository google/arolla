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
#ifndef AROLLA_OPERATORS_EXPERIMENTAL_DICT_H_
#define AROLLA_OPERATORS_EXPERIMENTAL_DICT_H_

#include <cmath>
#include <cstdint>
#include <optional>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/dict/dict_types.h"
#include "arolla/util/view_types.h"

namespace arolla {

// dict._make_key_to_row_dict operator constructs a dict from an array of keys
// into their positions in the array. Returns an error in case of duplicated
// keys.
struct MakeKeyToRowDictOp {
  template <typename Key>
  absl::StatusOr<KeyToRowDict<Key>> operator()(
      const DenseArray<Key>& keys) const {
    typename KeyToRowDict<Key>::Map dict;
    dict.reserve(keys.size());
    absl::Status status;
    keys.ForEach([&](int64_t id, bool present, view_type_t<Key> key) {
      if (present) {
        if constexpr (std::is_floating_point_v<Key>) {
          if (std::isnan(key)) {
            status = absl::InvalidArgumentError("NaN dict keys are prohibited");
            return;
          }
        }
        auto [iter, inserted] = dict.emplace(Key{key}, id);
        if (!inserted) {
          status = absl::InvalidArgumentError(
              absl::StrFormat("duplicated key %s in the dict", Repr(Key{key})));
        }
      } else {
        // TODO: Prohibit missing keys.
        // status = absl::InvalidArgumentError("missing key in the dict");
      }
    });
    if (status.ok()) {
      return KeyToRowDict<Key>(std::move(dict));
    } else {
      return status;
    }
  }
};

// dict._get_row operator applies given dict to point(s).
class DictGetRowOp {
 public:
  template <typename Key>
  OptionalValue<int64_t> operator()(const KeyToRowDict<Key>& dict,
                                    view_type_t<Key> key) const {
    if (auto iter = dict.map().find(key); iter != dict.map().end()) {
      return iter->second;
    } else {
      return std::nullopt;
    }
  }
};

// dict._contains operator implementation.
class DictContainsOp {
 public:
  template <typename Key>
  OptionalUnit operator()(const KeyToRowDict<Key>& dict,
                          view_type_t<Key> key) const {
    return OptionalUnit{dict.map().contains(key)};
  }
};

// dict._keys operator implementation.
class DictKeysOp {
 public:
  template <typename Key>
  absl::StatusOr<DenseArray<Key>> operator()(
      EvaluationContext* ctx, const KeyToRowDict<Key>& dict) const {
    DenseArrayBuilder<Key> result_builder(dict.map().size(),
                                          &ctx->buffer_factory());
    for (const auto& [key, row] : dict.map()) {
      if (row < 0 || row >= dict.map().size()) {
        return absl::InternalError(
            "unexpected row ids in the key-to-row mapping in the dict");
      }
      result_builder.Set(row, key);
    }

    DenseArray<Key> result = std::move(result_builder).Build();
    if (!result.IsFull()) {
      return absl::InternalError("incomplete key-to-row mapping in the dict");
    }
    return result;
  }
};

}  // namespace arolla

#endif  // AROLLA_OPERATORS_EXPERIMENTAL_DICT_H_
