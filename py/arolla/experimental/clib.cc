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
// Python extension module for memoryview of arolla dense arrays.

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

#include "py/arolla/abc/pybind11_utils.h"
#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11_abseil/absl_casters.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_value.h"

namespace arolla::python {
namespace {

namespace py = pybind11;

PYBIND11_MODULE(clib, m) {
  struct QValueBufferProxy {
    TypedValue qvalue;
    const void* ptr;
    ssize_t itemsize;
    ssize_t size;
    std::string format;
  };
  py::class_<QValueBufferProxy>(m, "_QValueBufferProxy", py::buffer_protocol())
      .def_buffer([](const QValueBufferProxy& proxy) {
        return py::buffer_info(const_cast<void*>(proxy.ptr), proxy.itemsize,
                               proxy.format,
                               /*ndim=*/1,
                               /*shape_in=*/{proxy.size},
                               /*strides_in=*/{proxy.itemsize},
                               /*readonly=*/true);
      });
  m.def(
      "get_dense_array_memoryview",
      [](const TypedValue& qvalue) {
        QValueBufferProxy result{qvalue};
        const auto setup_result = [&result](const auto& dense_array) {
          using T = typename std::decay_t<decltype(dense_array)>::base_type;
          if (!dense_array.IsFull()) {
            throw py::value_error(
                "dense array has missing elements, cannot provide a "
                "memoryview");
          }
          result.ptr = const_cast<T*>(dense_array.values.span().data());
          result.itemsize = sizeof(T);
          result.size = dense_array.size();
          result.format = py::format_descriptor<T>::format();
        };
        const auto& qtype = qvalue.GetType();
        if (qtype == GetDenseArrayQType<bool>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<bool>>());
        } else if (qtype == GetDenseArrayQType<float>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<float>>());
        } else if (qtype == GetDenseArrayQType<double>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<double>>());
        } else if (qtype == GetDenseArrayWeakFloatQType()) {
          setup_result(qvalue.UnsafeAs<DenseArray<double>>());
        } else if (qtype == GetDenseArrayQType<int32_t>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<int32_t>>());
        } else if (qtype == GetDenseArrayQType<int64_t>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<int64_t>>());
        } else if (qtype == GetDenseArrayQType<uint64_t>()) {
          setup_result(qvalue.UnsafeAs<DenseArray<uint64_t>>());
        } else {
          PyErr_Format(PyExc_NotImplementedError,
                       "cannot provide a memoryview (qtype=%s)",
                       std::string(qtype->name()).c_str());
          throw py::error_already_set();
        }
        return py::memoryview(py::cast(std::move(result)));
      },
      py::arg("dense_array"), py::pos_only(),
      py::doc("get_dense_array_memoryview(dense_array, /)\n--\n\n"
              "Returns a memoryview of the internal buffer of `dense_array`."));
}

}  // namespace
}  // namespace arolla::python
