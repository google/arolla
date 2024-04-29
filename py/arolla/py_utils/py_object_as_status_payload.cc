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
#include "py/arolla/py_utils/py_object_as_status_payload.h"

#include <algorithm>
#include <cstdio>
#include <utility>
#include <vector>

#include "absl/log/check.h"
#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"

// absl::MakeCordFromExternal creates an absl::Cord that owns an external
// memory. We use this mechanism to make the cord own a ref-counted PyObject.
//
// We found no simple way to access the memory address of the owned PyObject, so
// we encoded this address to the cord string data. In addition, to mitigate
// the risk of access to the py-object through a deep copy of the cord, we embed
// "self-raw-address" pointing to the string itself and a "magic-id" that is
// process specific:
//
//   <py_object_as_cord:self_raw_address:py_object_raw_address:magic_id>
//

namespace arolla::python {
namespace {

constexpr size_t kTokenMaxSize = 80;

// Returns a unique id specific to the current process.
unsigned int GetMagicId() {
  static const int result = [] {
    absl::BitGen bitgen;
    return absl::Uniform<unsigned int>(bitgen);
  }();
  return result;
}

}  // namespace

absl::StatusOr<absl::Cord> WrapPyObjectToCord(PyObjectGILSafePtr obj) {
  std::vector<char> token(kTokenMaxSize);
  const char* const self_raw_address = &token[0];
  const void* const py_object_raw_addres = obj.get();
  const int n =
      std::snprintf(&token[0], token.size(), "<py_object_as_cord:%p:%p:0x%08x>",
                    self_raw_address, py_object_raw_addres, GetMagicId());
  if (n < 0 || n >= kTokenMaxSize) {
    return absl::InternalError(
        "unable to generate a <py_object_as_cord> token");
  }
  token.resize(n);
  auto result = absl::MakeCordFromExternal(
      absl::string_view(self_raw_address, n),
      [token = std::move(token), obj = std::move(obj)](absl::string_view) {});
  if (!result.TryFlat().has_value() ||
      &result.TryFlat().value()[0] != self_raw_address) {
    return absl::InternalError("unable to format <py_object_as_cord> token");
  }
  return result;
}

absl::StatusOr<PyObjectGILSafePtr> UnwrapPyObjectFromCord(absl::Cord token) {
  const auto token_view = token.TryFlat();
  if (!token_view.has_value() || token_view->size() > kTokenMaxSize) {
    return absl::InvalidArgumentError("invalid <py_object_as_cord> token");
  }
  char buffer[kTokenMaxSize + 1];
  std::copy(token_view->begin(), token_view->end(), buffer);
  buffer[token_view->size()] = '\0';
  void* self_raw_address = nullptr;
  void* py_object_raw_addres = nullptr;
  unsigned int magic_id = 0;
  if (3 != std::sscanf(buffer, "<py_object_as_cord:%p:%p:0x%x>",
                       &self_raw_address, &py_object_raw_addres, &magic_id) ||
      self_raw_address != token_view->data() || magic_id != GetMagicId()) {
    return absl::InvalidArgumentError("invalid <py_object_as_cord> token");
  }
  return PyObjectGILSafePtr::NewRef(
      static_cast<PyObject*>(py_object_raw_addres));
}

absl::Status WritePyObjectToStatusPayload(absl::Status* status,
                                          absl::string_view type_url,
                                          PyObjectGILSafePtr obj) {
  DCHECK_NE(status, nullptr);
  if (obj == nullptr || obj.get() == Py_None) {
    status->ErasePayload(type_url);
    return absl::OkStatus();
  }
  auto token = WrapPyObjectToCord(std::move(obj));
  if (!token.ok()) {
    return token.status();
  }
  status->SetPayload(type_url, *std::move(token));
  return absl::OkStatus();
}

absl::StatusOr<PyObjectGILSafePtr> ReadPyObjectFromStatusPayload(
    const absl::Status& status, absl::string_view type_url) {
  auto token = status.GetPayload(type_url);
  if (!token.has_value()) {
    return PyObjectGILSafePtr{};
  }
  return UnwrapPyObjectFromCord(*token);
}

}  // namespace arolla::python
