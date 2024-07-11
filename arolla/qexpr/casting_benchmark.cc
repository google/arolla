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
#include <cstdint>

#include "benchmark/benchmark.h"
#include "absl/log/check.h"
#include "absl/types/span.h"
#include "arolla/array/edge.h"
#include "arolla/array/qtype/types.h"
#include "arolla/dense_array/edge.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/qexpr/casting.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/optional_qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/weak_qtype.h"
#include "arolla/util/init_arolla.h"

namespace arolla {
namespace {

void BM_FindMatchingSignature_MathAdd(benchmark::State& state) {
  InitArolla();
  const auto i32 = GetQType<int32_t>();
  const auto i64 = GetQType<int64_t>();
  const auto f32 = GetQType<float>();
  const auto f64 = GetQType<double>();
  const auto o_i32 = GetOptionalQType<int32_t>();
  const auto o_i64 = GetOptionalQType<int64_t>();
  const auto o_f32 = GetOptionalQType<float>();
  const auto o_f64 = GetOptionalQType<double>();
  const auto da_i32 = GetDenseArrayQType<int32_t>();
  const auto da_i64 = GetDenseArrayQType<int64_t>();
  const auto da_f32 = GetDenseArrayQType<float>();
  const auto da_f64 = GetDenseArrayQType<double>();
  const auto a_i32 = GetArrayQType<int32_t>();
  const auto a_i64 = GetArrayQType<int64_t>();
  const auto a_f32 = GetArrayQType<float>();
  const auto a_f64 = GetArrayQType<double>();
  const auto input_types = {f32, f64};
  const auto output_type = f64;
  const auto supported_signatures = {
      QExprOperatorSignature::Get({i32, i32}, i32),
      QExprOperatorSignature::Get({i64, i64}, i64),
      QExprOperatorSignature::Get({f32, f32}, f32),
      QExprOperatorSignature::Get({f64, f64}, f64),
      QExprOperatorSignature::Get({o_i32, o_i32}, o_i32),
      QExprOperatorSignature::Get({o_i64, o_i64}, o_i64),
      QExprOperatorSignature::Get({o_f32, o_f32}, o_f32),
      QExprOperatorSignature::Get({o_f64, o_f64}, o_f64),
      QExprOperatorSignature::Get({da_i32, da_i32}, da_i32),
      QExprOperatorSignature::Get({da_i64, da_i64}, da_i64),
      QExprOperatorSignature::Get({da_f32, da_f32}, da_f32),
      QExprOperatorSignature::Get({da_f64, da_f64}, da_f64),
      QExprOperatorSignature::Get({a_i32, a_i32}, a_i32),
      QExprOperatorSignature::Get({a_i64, a_i64}, a_i64),
      QExprOperatorSignature::Get({a_f32, a_f32}, a_f32),
      QExprOperatorSignature::Get({a_f64, a_f64}, a_f64),
  };
  for (auto _ : state) {
    auto signature = FindMatchingSignature(
        input_types, output_type, supported_signatures, "test.math.add");
    CHECK_OK(signature);
    benchmark::DoNotOptimize(signature);
  }
}

void BM_FindMatchingSignature_MathSum(benchmark::State& state) {
  InitArolla();
  const auto wf = GetWeakFloatQType();
  const auto o_i32 = GetOptionalQType<int32_t>();
  const auto o_i64 = GetOptionalQType<int64_t>();
  const auto o_f32 = GetOptionalQType<float>();
  const auto o_f64 = GetOptionalQType<double>();
  const auto da_i32 = GetDenseArrayQType<int32_t>();
  const auto da_i64 = GetDenseArrayQType<int64_t>();
  const auto da_f32 = GetDenseArrayQType<float>();
  const auto da_f64 = GetDenseArrayQType<double>();
  const auto da_edge = GetQType<DenseArrayEdge>();
  const auto da_edge_to_scalar = GetQType<DenseArrayGroupScalarEdge>();
  const auto a_i32 = GetArrayQType<int32_t>();
  const auto a_i64 = GetArrayQType<int64_t>();
  const auto a_f32 = GetArrayQType<float>();
  const auto a_f64 = GetArrayQType<double>();
  const auto a_edge = GetQType<ArrayEdge>();
  const auto a_edge_to_scalar = GetQType<ArrayGroupScalarEdge>();
  const auto input_types = {a_f32, a_edge_to_scalar, wf};
  const auto output_type = o_f32;
  const auto supported_signatures = {
      QExprOperatorSignature::Get({da_i32, da_edge, o_i32}, da_i32),
      QExprOperatorSignature::Get({da_i64, da_edge, o_i64}, da_i64),
      QExprOperatorSignature::Get({da_f32, da_edge, o_f32}, da_f32),
      QExprOperatorSignature::Get({da_f64, da_edge, o_f64}, da_f64),
      QExprOperatorSignature::Get({da_i32, da_edge_to_scalar, o_i32}, o_i32),
      QExprOperatorSignature::Get({da_i64, da_edge_to_scalar, o_i64}, o_i64),
      QExprOperatorSignature::Get({da_f32, da_edge_to_scalar, o_f32}, o_f32),
      QExprOperatorSignature::Get({da_f64, da_edge_to_scalar, o_f64}, o_f64),
      QExprOperatorSignature::Get({a_i32, a_edge, o_i32}, a_i32),
      QExprOperatorSignature::Get({a_i64, a_edge, o_i64}, a_i64),
      QExprOperatorSignature::Get({a_f32, a_edge, o_f32}, a_f32),
      QExprOperatorSignature::Get({a_f64, a_edge, o_f64}, a_f64),
      QExprOperatorSignature::Get({a_i32, a_edge_to_scalar, o_i32}, o_i32),
      QExprOperatorSignature::Get({a_i64, a_edge_to_scalar, o_i64}, o_i64),
      QExprOperatorSignature::Get({a_f32, a_edge_to_scalar, o_f32}, o_f32),
      QExprOperatorSignature::Get({a_f64, a_edge_to_scalar, o_f64}, o_f64),
  };
  for (auto _ : state) {
    auto signature = FindMatchingSignature(
        input_types, output_type, supported_signatures, "test.math.sum");
    CHECK_OK(signature);
    benchmark::DoNotOptimize(signature);
  }
}

BENCHMARK(BM_FindMatchingSignature_MathAdd);
BENCHMARK(BM_FindMatchingSignature_MathSum);

}  // namespace
}  // namespace arolla
