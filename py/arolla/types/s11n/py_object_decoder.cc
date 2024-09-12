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
#include "py/arolla/types/s11n/py_object_decoder.h"

#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "py/arolla/abc/py_object_qtype.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/types/qvalue/py_function_operator.h"
#include "py/arolla/types/s11n/codec_name.h"
#include "py/arolla/types/s11n/py_object_codec.pb.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/decoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprNodePtr;
using ::arolla::expr::ExprOperatorSignature;
using ::arolla::serialization_base::NoExtensionFound;
using ::arolla::serialization_base::ValueDecoderResult;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::RegisterValueDecoder;

absl::StatusOr<TypedValue> DecodePyObjectValue(
    const PyObjectV1Proto::PyObjectProto& py_object_value) {
  if (!py_object_value.has_data()) {
    return absl::InvalidArgumentError(
        "missing py_object.py_object_value.data; value=PY_OBJECT");
  }
  if (!py_object_value.has_codec()) {
    return absl::InvalidArgumentError(
        "missing py_object.py_object_value.codec; value=PY_OBJECT");
  }
  ASSIGN_OR_RETURN(
      auto result,
      DecodePyObject(py_object_value.data(), py_object_value.codec()),
      _ << "value=PY_OBJECT");
  return result;
}

absl::StatusOr<ValueDecoderResult> DecodePyFunctionOperator(
    const PyObjectV1Proto::PyFunctionOperatorProto& op_proto,
    absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!op_proto.has_name()) {
    return absl::InvalidArgumentError(
        "missing py_function_operator.name; value=PY_FUNCTION_OPERATOR");
  }
  if (!op_proto.has_signature_spec()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("missing py_function_operator.signature_spec; "
                        "value=PY_FUNCTION_OPERATOR with name=%s",
                        op_proto.name()));
  }
  if (input_values.empty()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected at least one input_value_index, got 0; "
                        "value=PY_FUNCTION_OPERATOR with name=%s",
                        op_proto.name()));
  }
  if (input_exprs.size() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected 1 input_expr_index, got %d; "
                        "value=PY_FUNCTION_OPERATOR with name=%s",
                        input_exprs.size(), op_proto.name()));
  }
  ASSIGN_OR_RETURN(
      auto signature,
      ExprOperatorSignature::Make(op_proto.signature_spec(),
                                  input_values.last(input_values.size() - 1)),
      _ << "value=PY_FUNCTION_OPERATOR with name=" << op_proto.name());

  ASSIGN_OR_RETURN(
      auto result,
      PyFunctionOperator::Make(op_proto.name(), std::move(signature),
                               op_proto.doc(), input_exprs[0], input_values[0]),
      _ << "value=PY_FUNCTION_OPERATOR with name=" << op_proto.name());
  return TypedValue::FromValue(result);
}

absl::StatusOr<ValueDecoderResult> DecodePyObjectQValue(
    const ValueProto& value_proto, absl::Span<const TypedValue> input_values,
    absl::Span<const ExprNodePtr> input_exprs) {
  if (!value_proto.HasExtension(PyObjectV1Proto::extension)) {
    return NoExtensionFound();
  }
  const auto& py_object_proto =
      value_proto.GetExtension(PyObjectV1Proto::extension);
  switch (py_object_proto.value_case()) {
    case PyObjectV1Proto::kPyObjectQtype:
      return TypedValue::FromValue(GetPyObjectQType());
    case PyObjectV1Proto::kPyObjectValue:
      return DecodePyObjectValue(py_object_proto.py_object_value());
    case PyObjectV1Proto::kPyFunctionOperatorValue:
      return DecodePyFunctionOperator(
          py_object_proto.py_function_operator_value(), input_values,
          input_exprs);
    case PyObjectV1Proto::VALUE_NOT_SET:
      return absl::InvalidArgumentError("missing value");
  }
  return absl::InvalidArgumentError(absl::StrFormat(
      "unexpected value=%d", static_cast<int>(py_object_proto.value_case())));
}

class PyObjectDecodingFnReg {
 public:
  static PyObjectDecodingFnReg& instance() {
    static absl::NoDestructor<PyObjectDecodingFnReg> result;
    return *result;
  }

  PyObjectDecodingFn Get() {
    absl::MutexLock lock(&mutex_);
    return decoding_fn_;
  }

  void Set(PyObjectDecodingFn decoding_fn) {
    absl::MutexLock lock(&mutex_);
    decoding_fn_ = std::move(decoding_fn);
  }

 private:
  absl::Mutex mutex_;
  PyObjectDecodingFn decoding_fn_;
};

}  // namespace

void RegisterPyObjectDecodingFn(PyObjectDecodingFn fn) {
  PyObjectDecodingFnReg::instance().Set(std::move(fn));
}

absl::StatusOr<TypedValue> DecodePyObject(absl::string_view data,
                                          std::string codec) {
  auto decoding_fn = PyObjectDecodingFnReg::instance().Get();
  if (decoding_fn == nullptr) {
    return absl::FailedPreconditionError(
        "no PyObject deserialization function has been registered");
  }
  AcquirePyGIL gil_acquire;
  ASSIGN_OR_RETURN(auto py_obj, decoding_fn(data, codec));
  return MakePyObjectQValue(PyObjectPtr::NewRef(py_obj), std::move(codec));
}

absl::Status InitPyObjectCodecDecoder() {
  return RegisterValueDecoder(kPyObjectV1Codec, DecodePyObjectQValue);
}

}  // namespace arolla::python
