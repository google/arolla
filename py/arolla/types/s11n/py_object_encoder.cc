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
#include "py/arolla/types/s11n/py_object_encoder.h"

#include <string>
#include <utility>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "py/arolla/abc/py_object_qtype.h"
#include "py/arolla/py_utils/py_utils.h"
#include "py/arolla/types/qvalue/py_function_operator.h"
#include "py/arolla/types/s11n/codec_name.h"
#include "py/arolla/types/s11n/py_object_codec.pb.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_ref.h"
#include "arolla/serialization_base/encoder.h"
#include "arolla/serialization_codecs/registry.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::python {
namespace {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::serialization_base::Encoder;
using ::arolla::serialization_base::ValueProto;
using ::arolla::serialization_codecs::RegisterValueEncoderByQType;
using ::arolla::serialization_codecs::
    RegisterValueEncoderByQValueSpecialisationKey;

absl::StatusOr<ValueProto> GenValueProto(Encoder& encoder) {
  ASSIGN_OR_RETURN(auto codec_index, encoder.EncodeCodec(kPyObjectV1Codec));
  ValueProto value_proto;
  value_proto.set_codec_index(codec_index);
  return value_proto;
}

absl::StatusOr<ValueProto> EncodePyObjectQValue(TypedRef value,
                                                Encoder& encoder) {
  if (value.GetType() == GetQType<QTypePtr>()) {
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
    value_proto.MutableExtension(PyObjectV1Proto::extension)
        ->set_py_object_qtype(true);
    return value_proto;
  } else if (value.GetType() == GetPyObjectQType()) {
    ASSIGN_OR_RETURN(auto maybe_codec, GetPyObjectCodec(value));
    if (!maybe_codec) {
      return absl::InvalidArgumentError(
          absl::StrFormat("missing serialization codec for %s", value.Repr()));
    }
    ASSIGN_OR_RETURN(auto data, EncodePyObject(value));
    ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
    auto* py_obj_proto =
        value_proto.MutableExtension(PyObjectV1Proto::extension);
    py_obj_proto->mutable_py_object_value()->set_codec(*maybe_codec);
    py_obj_proto->mutable_py_object_value()->set_data(data);
    return value_proto;
  } else {
    return absl::UnimplementedError(absl::StrFormat(
        "%s does not support serialization of %s: %s", kPyObjectV1Codec,
        value.GetType()->name(), value.Repr()));
  }
}

absl::StatusOr<ValueProto> EncodePyFunctionOperator(TypedRef value,
                                                    Encoder& encoder) {
  if (value.GetType() != GetQType<ExprOperatorPtr>()) {
    return absl::UnimplementedError(absl::StrFormat(
        "%s does not support serialization of %s: %s", kPyObjectV1Codec,
        value.GetType()->name(), value.Repr()));
  }
  const auto& op_value = *value.UnsafeAs<ExprOperatorPtr>();
  if (typeid(op_value) != typeid(PyFunctionOperator)) {
    return absl::UnimplementedError(absl::StrFormat(
        "%s does not support serialization of %s: %s", kPyObjectV1Codec,
        value.GetType()->name(), value.Repr()));
  }
  const auto& op = static_cast<const PyFunctionOperator&>(op_value);
  ASSIGN_OR_RETURN(auto value_proto, GenValueProto(encoder));
  auto* op_proto = value_proto.MutableExtension(PyObjectV1Proto::extension)
                       ->mutable_py_function_operator_value();
  const auto& name = op.display_name();
  op_proto->set_name(name.data(), name.size());
  op_proto->set_signature_spec(GetExprOperatorSignatureSpec(op.signature()));
  op_proto->set_doc(op.doc());
  // Encode and set the objects.
  ASSIGN_OR_RETURN(auto encoded_qtype_inference_expr,
                   encoder.EncodeExpr(op.GetQTypeInferenceExpr()),
                   _ << "GetQTypeInferenceExpr(); value=PY_FUNCTION_OPERATOR "
                        "with name="
                     << op.display_name());
  ASSIGN_OR_RETURN(
      auto encoded_eval_fn, encoder.EncodeValue(op.GetPyEvalFn()),
      _ << "py_obj=PyEvalFn(); value=PY_FUNCTION_OPERATOR with name="
        << op.display_name());
  value_proto.add_input_expr_indices(encoded_qtype_inference_expr);
  value_proto.add_input_value_indices(encoded_eval_fn);
  // Sets default values for the operator signature.
  for (const auto& param : op.signature().parameters) {
    if (param.default_value.has_value()) {
      ASSIGN_OR_RETURN(auto value_index,
                       encoder.EncodeValue(param.default_value.value()));
      value_proto.add_input_value_indices(value_index);
    }
  }
  return value_proto;
}

class PyObjectEncodingFnReg {
 public:
  static PyObjectEncodingFnReg& instance() {
    static absl::NoDestructor<PyObjectEncodingFnReg> result;
    return *result;
  }

  PyObjectEncodingFn Get() {
    absl::MutexLock lock(&mutex_);
    return encoding_fn_;
  }

  void Set(PyObjectEncodingFn encoding_fn) {
    absl::MutexLock lock(&mutex_);
    encoding_fn_ = std::move(encoding_fn);
  }

 private:
  absl::Mutex mutex_;
  PyObjectEncodingFn encoding_fn_;
};

}  // namespace

void RegisterPyObjectEncodingFn(PyObjectEncodingFn fn) {
  PyObjectEncodingFnReg::instance().Set(std::move(fn));
}

absl::StatusOr<std::string> EncodePyObject(TypedRef value) {
  auto encoding_fn = PyObjectEncodingFnReg::instance().Get();
  if (encoding_fn == nullptr) {
    return absl::FailedPreconditionError(
        "no PyObject serialization function has been registered");
  }
  ASSIGN_OR_RETURN(auto maybe_codec, GetPyObjectCodec(value));
  if (!maybe_codec.has_value()) {
    return absl::InvalidArgumentError(
        absl::StrFormat("missing serialization codec for %s", value.Repr()));
  }
  ASSIGN_OR_RETURN(const PyObjectGILSafePtr& py_obj, GetPyObjectValue(value));
  return encoding_fn(py_obj.get(), *maybe_codec);
}

absl::Status InitPyObjectCodecEncoder() {
  RETURN_IF_ERROR(RegisterValueEncoderByQValueSpecialisationKey(
      "::arolla::python::PyFunctionOperator", EncodePyFunctionOperator));
  RETURN_IF_ERROR(
      RegisterValueEncoderByQType(GetPyObjectQType(), EncodePyObjectQValue));
  return absl::OkStatus();
}

}  // namespace arolla::python
