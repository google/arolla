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
// Python extension module that introduces types for testing GIL behavior
// during destruction.
//

#include <Python.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/strings/string_view.h"
#include "arolla/memory/frame.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/repr.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"

namespace {

//
// Definition of C++ types.
//

std::atomic<int64_t> g_dtor_called_with_gil = 0;
std::atomic<int64_t> g_dtor_called_without_gil = 0;

struct GilDtor {
  bool is_moved_from = false;

  GilDtor() = default;
  GilDtor(const GilDtor&) = default;
  GilDtor& operator=(const GilDtor&) = default;

  GilDtor(GilDtor&& rhs) { rhs.is_moved_from = true; }
  GilDtor& operator=(GilDtor&& rhs) {
    is_moved_from = false;
    rhs.is_moved_from = true;
    return *this;
  }

  ~GilDtor() {
    if (is_moved_from) {
      // Destroying moved-from objects is cheap, and we don't want to track
      // if that happens under GIL (for example, we destroy a moved-from
      // object in make_trivially_copyable_qvalue method below which runs
      // under GIL).
      return;
    }
    if (PyGILState_Check() != 0) {
      g_dtor_called_with_gil.fetch_add(1, std::memory_order_relaxed);
    } else {
      g_dtor_called_without_gil.fetch_add(1, std::memory_order_relaxed);
    }
  }
};

class GilDtorQType final : public arolla::QType {
 public:
  GilDtorQType(std::string name, bool is_trivially_copyable)
      : QType(ConstructorArgs{
            .name = std::move(name),
            .type_info = typeid(GilDtor),
            .type_layout = ::arolla::MakeTypeLayout<GilDtor>(),
            .is_trivially_copyable = is_trivially_copyable,
        }) {}

  void UnsafeCopy(const void* source, void* destination) const final {
    *static_cast<GilDtor*>(destination) = *static_cast<const GilDtor*>(source);
  }

  void UnsafeCombineToFingerprintHasher(
      const void* source, ::arolla::FingerprintHasher* hasher) const final {}
};

::arolla::QTypePtr GetTriviallyCopyableGilDtorQType() {
  static const absl::NoDestructor<GilDtorQType> result(
      "TRIVIALLY_COPYABLE_GIL_DTOR", /*is_trivially_copyable=*/true);
  return result.get();
}

::arolla::QTypePtr GetNonTriviallyCopyableGilDtorQType() {
  static const absl::NoDestructor<GilDtorQType> result(
      "NON_TRIVIALLY_COPYABLE_GIL_DTOR", /*is_trivially_copyable=*/false);
  return result.get();
}

}  // namespace

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(testing_gil_types, m) {
  m.def(
      "make_trivially_copyable_qvalue",
      [] {
        return *TypedValue::FromValueWithQType(
            GilDtor{}, GetTriviallyCopyableGilDtorQType());
      },
      py::doc("Returns a QValue whose QType is trivially copyable."));
  m.def(
      "make_non_trivially_copyable_qvalue",
      [] {
        return *TypedValue::FromValueWithQType(
            GilDtor{}, GetNonTriviallyCopyableGilDtorQType());
      },
      py::doc("Returns a QValue whose QType is not trivially copyable."));

  m.def(
      "count_dtor_called_with_gil",
      [] { return g_dtor_called_with_gil.load(std::memory_order_relaxed); },
      py::doc("Returns the number of times a GilDtor was destroyed with GIL."));
  m.def(
      "count_dtor_called_without_gil",
      [] { return g_dtor_called_without_gil.load(std::memory_order_relaxed); },
      py::doc(
          "Returns the number of times a GilDtor was destroyed without GIL."));
}

}  // namespace
}  // namespace arolla::python
