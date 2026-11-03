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
#include "py/arolla/py_utils/py_hash.h"

#include <Python.h>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "py/arolla/py_utils/py_utils.h"

namespace arolla::python {
namespace {

#if PY_VERSION_HEX >= 0x030E0000
#define _Py_HashBytes Py_HashBuffer
#else
extern "C" Py_hash_t _Py_HashBytes(const void* src, Py_ssize_t len);
#endif

ABSL_ATTRIBUTE_ALWAYS_INLINE Py_uhash_t
ReduceUInt64ModPyHashModulus(uint64_t value) {
  static_assert(_PyHASH_BITS >= 31 && _PyHASH_BITS < 64);
  static_assert(sizeof(Py_uhash_t) == sizeof(uint64_t));
  return value % _PyHASH_MODULUS;
}

Py_hash_t PyHashUInt64Fast(uint64_t value) {
  const Py_uhash_t x = ReduceUInt64ModPyHashModulus(value);
  return static_cast<Py_hash_t>(x == static_cast<Py_uhash_t>(-1) ? -2 : x);
}

Py_hash_t PyHashInt64Fast(int64_t value) {
  if (value >= 0) {
    return PyHashUInt64Fast(static_cast<uint64_t>(value));
  }
  const uint64_t abs_value = 0ULL - static_cast<uint64_t>(value);
  const Py_uhash_t x = 0ULL - ReduceUInt64ModPyHashModulus(abs_value);
  return static_cast<Py_hash_t>(x == static_cast<Py_uhash_t>(-1) ? -2 : x);
}

Py_hash_t PyHashInt64Fallback(int64_t value) {
  static_assert(sizeof(long long) >= sizeof(int64_t));  // NOLINT
  auto py_int = PyObjectPtr::Own(PyLong_FromLongLong(value));
  if (py_int == nullptr) [[unlikely]] {
    PyErr_Clear();
    return PyHashInt64Fast(value);
  }
  return PyObject_Hash(py_int.get());
}

Py_hash_t PyHashUInt64Fallback(uint64_t value) {
  static_assert(sizeof(unsigned long long) >= sizeof(uint64_t));  // NOLINT
  auto py_int = PyObjectPtr::Own(PyLong_FromUnsignedLongLong(value));
  if (py_int == nullptr) [[unlikely]] {
    PyErr_Clear();
    return PyHashUInt64Fast(value);
  }
  return PyObject_Hash(py_int.get());
}

bool CheckFastIntHashCompatibility() {
  constexpr int64_t kInt64Probes[] = {
      0,
      1,
      -1,
      -2,
      static_cast<int64_t>(_PyHASH_MODULUS),
      -static_cast<int64_t>(_PyHASH_MODULUS),
      -static_cast<int64_t>(_PyHASH_MODULUS) - 1,
      std::numeric_limits<int64_t>::max(),
      std::numeric_limits<int64_t>::min(),
  };
  for (int64_t v : kInt64Probes) {
    if (PyHashInt64Fast(v) != PyHashInt64Fallback(v)) {
      return false;
    }
  }
  constexpr uint64_t kUInt64Probes[] = {
      0ULL,
      1ULL,
      _PyHASH_MODULUS,
      _PyHASH_MODULUS + 1,
      std::numeric_limits<uint64_t>::max(),
  };
  for (uint64_t v : kUInt64Probes) {
    if (PyHashUInt64Fast(v) != PyHashUInt64Fallback(v)) {
      return false;
    }
  }
  return true;
}

void WarnFastHashDivergence(const char* message) {
  PyObjectPtr prev_exc = PyErr_FetchRaisedException();
  if (PyErr_WarnEx(PyExc_RuntimeWarning, message, 1) < 0) {
    PyErr_WriteUnraisable(nullptr);
  }
  if (prev_exc != nullptr) {
    PyErr_RestoreRaisedException(std::move(prev_exc));
  }
}

bool IsFastIntHashCompatible() {
  static const bool kIsCompatible = [] {
    if (CheckFastIntHashCompatibility()) [[likely]] {
      return true;
    }
    WarnFastHashDivergence(
        "arolla::python::PyHashInt64/PyHashUInt64 fast path diverged from "
        "CPython long_hash; falling back to PyObject_Hash.");
    return false;
  }();
  return kIsCompatible;
}

struct PyHashSecret {
  uint64_t k0;
  uint64_t k1;
  unsigned char padding[8];
};
extern "C" PyHashSecret _Py_HashSecret;

// Streams code points into CPython's SipHash (matching Py_HashBuffer on
// PyUnicode_DATA) without allocating or copying a UCS-1/UCS-2/UCS-4 buffer.
class PySipHashStreamer {
 public:
  static_assert(std::endian::native == std::endian::little);
  static_assert(Py_HASH_CUTOFF == 0);
  static_assert(Py_HASH_ALGORITHM == Py_HASH_SIPHASH13 ||
                Py_HASH_ALGORITHM == Py_HASH_SIPHASH24);

  PySipHashStreamer() {
    const uint64_t k0 = _Py_HashSecret.k0;
    const uint64_t k1 = _Py_HashSecret.k1;
    v0_ = k0 ^ 0x736f6d6570736575ULL;
    v1_ = k1 ^ 0x646f72616e646f6dULL;
    v2_ = k0 ^ 0x6c7967656e657261ULL;
    v3_ = k1 ^ 0x7465646279746573ULL;
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE void AddRawBlock(uint64_t block) {
    AddBlock(block);
    total_bytes_ += 8;
  }

  template <int kKind>
  ABSL_ATTRIBUTE_ALWAYS_INLINE void AddAscii8(uint64_t word) {
    static_assert(kKind == 1 || kKind == 2 || kKind == 4);
    if constexpr (kKind == 1) {
      AddBlock(word);
      total_bytes_ += 8;
    } else if constexpr (kKind == 2) {
      auto widen4 = [](uint64_t x) {
        x = (x | (x << 16)) & 0x0000FFFF0000FFFFULL;
        return (x | (x << 8)) & 0x00FF00FF00FF00FFULL;
      };
      AddBlock(widen4(word & 0xFFFFFFFFULL));
      AddBlock(widen4(word >> 32));
      total_bytes_ += 16;
    } else {
      auto widen2 = [](uint64_t x) {
        return (x & 0xFFULL) | ((x & 0xFF00ULL) << 24);
      };
      AddBlock(widen2(word));
      AddBlock(widen2(word >> 16));
      AddBlock(widen2(word >> 32));
      AddBlock(widen2(word >> 48));
      total_bytes_ += 32;
    }
  }

  template <int kKind>
  ABSL_ATTRIBUTE_ALWAYS_INLINE void AddCodePoint(uint32_t cp) {
    static_assert(kKind == 1 || kKind == 2 || kKind == 4);
    acc_ |= static_cast<uint64_t>(cp) << shift_;
    shift_ += kKind * 8;
    if (shift_ == 64) {
      AddBlock(acc_);
      acc_ = 0;
      shift_ = 0;
      total_bytes_ += 8;
    }
  }

  Py_hash_t Finish() && {
    const uint64_t total_bytes = total_bytes_ + (shift_ >> 3);
    const uint64_t b = (total_bytes << 56) | acc_;
    v3_ ^= b;
    Round();
    v0_ ^= b;
    v2_ ^= 0xff;
    Round();
    Round();
#if Py_HASH_ALGORITHM == Py_HASH_SIPHASH13
    Round();
#endif
    const auto h = static_cast<Py_hash_t>((v0_ ^ v1_) ^ (v2_ ^ v3_));
    return h == -1 ? -2 : h;
  }

 private:
  ABSL_ATTRIBUTE_ALWAYS_INLINE void AddBlock(uint64_t mi) {
    v3_ ^= mi;
    Round();
    v0_ ^= mi;
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE void Round() {
    HalfRound(v0_, v1_, v2_, v3_, 13, 16);
    HalfRound(v2_, v1_, v0_, v3_, 17, 21);
#if Py_HASH_ALGORITHM == Py_HASH_SIPHASH24
    HalfRound(v0_, v1_, v2_, v3_, 13, 16);
    HalfRound(v2_, v1_, v0_, v3_, 17, 21);
#endif
  }

  ABSL_ATTRIBUTE_ALWAYS_INLINE static void HalfRound(uint64_t& a, uint64_t& b,
                                                     uint64_t& c, uint64_t& d,
                                                     int s, int t) {
    a += b;
    c += d;
    b = std::rotl(b, s) ^ a;
    d = std::rotl(d, t) ^ c;
    a = std::rotl(a, 32);
  }

  uint64_t v0_;
  uint64_t v1_;
  uint64_t v2_;
  uint64_t v3_;
  uint64_t acc_ = 0;
  int shift_ = 0;
  size_t total_bytes_ = 0;
};

template <int kKind>
ABSL_ATTRIBUTE_ALWAYS_INLINE uint32_t DecodeCodePointUnchecked(
    const uint8_t*& p) {
  uint32_t b0 = p[0];
  if (b0 < 0x80) {
    ++p;
    return b0;
  }
  if (kKind == 1 || b0 < 0xE0) {
    uint32_t cp = ((b0 & 0x1F) << 6) | (p[1] & 0x3F);
    p += 2;
    return cp;
  }
  if (kKind == 2 || b0 < 0xF0) {
    uint32_t cp = ((b0 & 0x0F) << 12) | ((p[1] & 0x3F) << 6) | (p[2] & 0x3F);
    p += 3;
    return cp;
  }
  uint32_t cp = ((b0 & 0x07) << 18) | ((p[1] & 0x3F) << 12) |
                ((p[2] & 0x3F) << 6) | (p[3] & 0x3F);
  p += 4;
  return cp;
}

template <int kKind>
Py_hash_t HashUtf8AsUcs(absl::string_view value) {
  static_assert(kKind == 1 || kKind == 2 || kKind == 4);
  const auto* p = reinterpret_cast<const uint8_t*>(value.data());
  const auto* const end = p + value.size();
  PySipHashStreamer hasher;
  constexpr ptrdiff_t kMaxBytesPerBlock =
      kKind == 1 ? 16 : (kKind == 2 ? 12 : 8);
  while (p + kMaxBytesPerBlock <= end) {
    uint64_t word;
    std::memcpy(&word, p, sizeof(word));
    if ((word & 0x8080808080808080ULL) == 0) {
      hasher.AddAscii8<kKind>(word);
      p += 8;
      continue;
    }
    uint64_t block = 0;
    for (int shift = 0; shift < 64; shift += kKind * 8) {
      block |= static_cast<uint64_t>(DecodeCodePointUnchecked<kKind>(p))
               << shift;
    }
    hasher.AddRawBlock(block);
  }
  while (p < end) {
    uint32_t cp = *p++;
    if (cp >= 0x80) {
      if (kKind == 1 || cp < 0xE0) {
        if (p == end) [[unlikely]] {
          break;
        }
        cp = ((cp & 0x1F) << 6) | (*p++ & 0x3F);
      } else if (kKind == 2 || cp < 0xF0) {
        if (p + 1 >= end) [[unlikely]] {
          break;
        }
        cp = ((cp & 0x0F) << 12) | ((p[0] & 0x3F) << 6) | (p[1] & 0x3F);
        p += 2;
      } else {
        if (p + 2 >= end) [[unlikely]] {
          break;
        }
        cp = ((cp & 0x07) << 18) | ((p[0] & 0x3F) << 12) |
             ((p[1] & 0x3F) << 6) | (p[2] & 0x3F);
        p += 3;
      }
    }
    hasher.AddCodePoint<kKind>(cp);
  }
  return std::move(hasher).Finish();
}

Py_hash_t PyHashUtf8Fast(absl::string_view value) {
  uint8_t max_byte = 0;
  for (unsigned char c : value) {
    max_byte = std::max<uint8_t>(max_byte, c);
  }
  // Fast path for ASCII: PyUnicode_DATA is identical to the UTF-8 byte buffer.
  if (max_byte < 0x80) [[likely]] {
    return _Py_HashBytes(value.data(), value.size());
  }
  if (max_byte < 0xC4) {
    // UCS-1 (Latin-1): max code point <= 0xFF (1- or 2-byte UTF-8 sequences).
    return HashUtf8AsUcs<1>(value);
  }
  if (max_byte < 0xF0) {
    // UCS-2 (BMP): max code point <= 0xFFFF (1-, 2-, or 3-byte UTF-8).
    return HashUtf8AsUcs<2>(value);
  }
  // UCS-4: max code point >= 0x10000 (1-, 2-, 3-, or 4-byte UTF-8).
  return HashUtf8AsUcs<4>(value);
}

Py_hash_t PyHashUtf8Fallback(absl::string_view value) {
  auto py_str =
      PyObjectPtr::Own(PyUnicode_FromStringAndSize(value.data(), value.size()));
  if (py_str == nullptr) [[unlikely]] {
    PyErr_Clear();
    return _Py_HashBytes(value.data(), value.size());
  }
  return PyObject_Hash(py_str.get());
}

bool CheckFastUtf8HashCompatibility() {
  constexpr absl::string_view kProbes[] = {"", "ascii", "café", "привет", "🎉"};
  for (absl::string_view s : kProbes) {
    if (PyHashUtf8Fast(s) != PyHashUtf8Fallback(s)) {
      return false;
    }
  }
  return true;
}

bool IsFastUtf8HashCompatible() {
  static const bool kIsCompatible = [] {
    if (CheckFastUtf8HashCompatibility()) [[likely]] {
      return true;
    }
    WarnFastHashDivergence(
        "arolla::python::PyHashUtf8 fast path diverged from CPython "
        "unicode_hash; falling back to PyObject_Hash.");
    return false;
  }();
  return kIsCompatible;
}

}  // namespace

Py_hash_t PyHashInt64(int64_t value) {
  DCheckPyGIL();
  if (IsFastIntHashCompatible()) [[likely]] {
    return PyHashInt64Fast(value);
  }
  return PyHashInt64Fallback(value);
}

Py_hash_t PyHashUInt64(uint64_t value) {
  DCheckPyGIL();
  if (IsFastIntHashCompatible()) [[likely]] {
    return PyHashUInt64Fast(value);
  }
  return PyHashUInt64Fallback(value);
}

Py_hash_t PyHashDouble(double value) {
  DCheckPyGIL();
  return _Py_HashDouble(nullptr, value);
}

Py_hash_t PyHashBytes(absl::string_view value) {
  DCheckPyGIL();
  return _Py_HashBytes(value.data(), value.size());
}

Py_hash_t PyHashUtf8(absl::string_view value) {
  DCheckPyGIL();
  if (IsFastUtf8HashCompatible()) [[likely]] {
    return PyHashUtf8Fast(value);
  }
  return PyHashUtf8Fallback(value);
}

bool IsFastPyHashCompatible() {
  DCheckPyGIL();
  return IsFastIntHashCompatible() && IsFastUtf8HashCompatible();
}

}  // namespace arolla::python
