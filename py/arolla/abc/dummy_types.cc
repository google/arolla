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
// Python extension module that introduces a few dummy arolla types.
//

#include <tuple>

#include "absl/base/no_destructor.h"
#include "absl/strings/string_view.h"
#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/simple_qtype.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/util/fingerprint.h"
#include "arolla/util/meta.h"
#include "arolla/util/repr.h"
#include "arolla/util/struct_field.h"

namespace {

//
// Definition of C++ types.
//

struct DummyValue {
  static int instance_counter;

  float x = 0;
  int y = 0;

  DummyValue() { instance_counter += 1; }

  ~DummyValue() { instance_counter -= 1; }

  DummyValue(const DummyValue& rhs) : x(rhs.x), y(rhs.y) {
    instance_counter += 1;
  }

  static auto ArollaStructFields() {
    using CppType = DummyValue;
    return std::tuple{
        AROLLA_DECLARE_STRUCT_FIELD(x),
        AROLLA_DECLARE_STRUCT_FIELD(y),
    };
  }
};

struct DummyContainer {};

int DummyValue::instance_counter = 0;

}  // namespace

namespace arolla {

//
// Define qtypes.
//

AROLLA_DECLARE_QTYPE(DummyValue);
AROLLA_DECLARE_QTYPE(DummyContainer);

AROLLA_DECLARE_REPR(DummyValue);
AROLLA_DECLARE_REPR(DummyContainer);

AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DummyValue);
AROLLA_DECLARE_FINGERPRINT_HASHER_TRAITS(DummyContainer);

ReprToken ReprTraits<DummyValue>::operator()(const DummyValue&) const {
  return ReprToken{"dummy-value"};
}

ReprToken ReprTraits<DummyContainer>::operator()(const DummyContainer&) const {
  return ReprToken{"dummy-container"};
}

void FingerprintHasherTraits<DummyValue>::operator()(
    FingerprintHasher* hasher, const DummyValue& value) const {
  hasher->Combine(absl::string_view("DummyValue"), value.x, value.y);
}

void FingerprintHasherTraits<DummyContainer>::operator()(
    FingerprintHasher* hasher, const DummyContainer&) const {
  hasher->Combine(absl::string_view("DummyContainer"));
}

QTypePtr QTypeTraits<DummyValue>::type() {
  struct DummyValueQType final : SimpleQType {
    DummyValueQType()
        : SimpleQType(meta::type<DummyValue>(), "DUMMY_VALUE",
                      /*value_qtype=*/nullptr,
                      /*qtype_specialization_key=*/
                      "::arolla::testing::DummyValueQType") {}
    absl::string_view UnsafePyQValueSpecializationKey(
        const void* /*source*/) const final {
      return "::arolla::testing::DummyValue";
    }
  };
  static const absl::NoDestructor<DummyValueQType> result;
  return result.get();
}

QTypePtr QTypeTraits<DummyContainer>::type() {
  struct DummyContainerQType final : SimpleQType {
    DummyContainerQType()
        : SimpleQType(meta::type<DummyContainer>(), "DUMMY_CONTAINER",
                      /*value_qtype=*/GetQType<DummyValue>(),
                      /*qtype_specialization_key=*/
                      "::arolla::testing::DummyContainerQType") {}
    absl::string_view UnsafePyQValueSpecializationKey(
        const void* /*source*/) const final {
      return "::arolla::testing::DummyContainer";
    }
  };
  static const absl::NoDestructor<DummyContainerQType> result;
  return result.get();
}

}  // namespace arolla

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(dummy_types, m) {
  m.def(
      "make_dummy_value", [] { return TypedValue::FromValue(DummyValue{}); },
      py::doc("Returns a new DummyValue instance."));

  m.def(
      "make_dummy_container",
      [] { return TypedValue::FromValue(DummyContainer{}); },
      py::doc("Returns a new DummyContainer instance."));

  m.def(
      "count_dummy_value_instances",
      [] { return DummyValue::instance_counter; },
      py::doc("Returns the number of DummyValue instances."));
}

}  // namespace
}  // namespace arolla::python
