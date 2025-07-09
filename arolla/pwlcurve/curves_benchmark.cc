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
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "arolla/pwlcurve/curves.h"

namespace arolla::pwlcurve {
namespace {

void BM_Parse(benchmark::State& state) {
  for (auto s : state) {
    CurveType t;
    std::vector<Point<double>> points;
    auto res = internals::Parse(';',
                                "PWLCurve({{0.1;0.1};{1.1;1.1};{2.1;2.1};"
                                "{3.1;3.1};{4.1;4.1};{5.1;5.1};{6.1;6.1}})",
                                &t, &points);
    benchmark::DoNotOptimize(res);
  }
}
BENCHMARK(BM_Parse);

template <CurveType type, typename FloatType>
void BM_Eval(benchmark::State& state) {
  // Test a typical production case: A Curve is constructed from parameters, and
  // then evaluated many times on different input.
  auto curve =
      NewCurve(type, {{1.0, 1.0}, {2.0, 10.0}, {10.0, 11.0}, {11.0, 13.0}})
          .value();

  for (auto s : state) {
    for (FloatType v = 0.0; v < 15.0; v += 0.0001) {
      auto res = curve->Eval(v);
      benchmark::DoNotOptimize(res);
    }
  }
}

BENCHMARK(BM_Eval<PWLCurve, float>);
BENCHMARK(BM_Eval<PWLCurve, double>);

BENCHMARK(BM_Eval<LogPWLCurve, float>);
BENCHMARK(BM_Eval<LogPWLCurve, double>);

BENCHMARK(BM_Eval<Log1pPWLCurve, float>);
BENCHMARK(BM_Eval<Log1pPWLCurve, double>);

BENCHMARK(BM_Eval<Symlog1pPWLCurve, float>);
BENCHMARK(BM_Eval<Symlog1pPWLCurve, double>);

// Generate a set of unique curves for benchmarking the parser (below).
std::vector<std::string> MakeUniqueCurves(int n) {
  std::vector<std::string> curves;
  curves.reserve(n);
  for (int ii = 0; ii < n; ++ii) {
    double x = (ii % n + 0.5) / n;
    curves.push_back(absl::StrFormat(
        "PWLCurve({{-100;0};{0;%.2f};{0.5;%.2f};{1;%.2f};{100;2}})", 0.5 * x, x,
        2.0 * x));
  }
  return curves;
}

template <typename FloatType>
inline void BM_CurveEvaluationNoCaching(benchmark::State& state) {
  const int num_unique_curves = state.range(0);

  double sum = 0.0;
  int i = 0;
  absl::BitGen rnd;  // Note: this is not seeded and not deterministic.
  std::vector<std::string> unique_curves = MakeUniqueCurves(num_unique_curves);
  for (auto s : state) {
    FloatType x = absl::Uniform<FloatType>(absl::IntervalOpen, rnd, 0, 1);
    const std::string& curve_expr = unique_curves[i % num_unique_curves];
    auto curve = NewCurve(curve_expr).value();
    sum += curve->Eval(x);
    ++i;
  }
  benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_CurveEvaluationNoCaching<float>)->Range(1, 100);
BENCHMARK(BM_CurveEvaluationNoCaching<double>)->Range(1, 100);

}  // namespace
}  // namespace arolla::pwlcurve
