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
#ifndef AROLLA_UTIL_ITERATOR_H_
#define AROLLA_UTIL_ITERATOR_H_

#include <iterator>

namespace arolla {

// ConstArrayIterator is a template for generating constant random-access
// iterators over constant array-like objects, that is, immutable objects which
// implement operator[].
//
// The Array class is required to define value_type, size_type,
// difference_type, and operator[](size_type). operator[] may return values
// either by const reference or by value.
//
// Binary operations such as (a - b) and (a == b) have undefined behavior if
// they are iterating over different Arrays.
template <typename Array>
class ConstArrayIterator {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using value_type = typename Array::value_type;
  using pointer = const value_type*;
  using reference = decltype(std::declval<const Array>()[0]);
  using size_type = typename Array::size_type;
  using difference_type = typename Array::difference_type;

  ConstArrayIterator() : arr_(nullptr), pos_(0) {}
  ConstArrayIterator(const Array* arr, size_type pos) : arr_(arr), pos_(pos) {}

  reference operator*() const { return (*arr_)[pos_]; }
  pointer operator->() const { return &**this; }
  reference operator[](size_type n) const { return *(*this + n); }
  ConstArrayIterator& operator+=(difference_type n) {
    pos_ += n;
    return *this;
  }
  ConstArrayIterator& operator-=(difference_type n) { return *this += -n; }
  ConstArrayIterator& operator++() { return *this += 1; }
  ConstArrayIterator& operator--() { return *this -= 1; }
  ConstArrayIterator operator++(int) {
    ConstArrayIterator t = *this;
    ++*this;
    return t;
  }
  ConstArrayIterator operator--(int) {
    ConstArrayIterator t = *this;
    --*this;
    return t;
  }
  friend ConstArrayIterator operator+(ConstArrayIterator v, difference_type n) {
    return v += n;
  }
  friend ConstArrayIterator operator+(difference_type n, ConstArrayIterator v) {
    return v += n;
  }
  friend ConstArrayIterator operator-(ConstArrayIterator v, difference_type n) {
    return v -= n;
  }
  friend difference_type operator-(const ConstArrayIterator& a,
                                   const ConstArrayIterator& b) {
    return static_cast<difference_type>(a.pos_) -
           static_cast<difference_type>(b.pos_);
  }
  friend bool operator==(ConstArrayIterator a, ConstArrayIterator b) {
    return a.pos_ == b.pos_;
  }
  friend bool operator!=(ConstArrayIterator a, ConstArrayIterator b) {
    return !(a == b);
  }
  friend bool operator<(ConstArrayIterator a, ConstArrayIterator b) {
    return b - a > 0;
  }
  friend bool operator>(ConstArrayIterator a, ConstArrayIterator b) {
    return b < a;
  }
  friend bool operator<=(ConstArrayIterator a, ConstArrayIterator b) {
    return !(a > b);
  }
  friend bool operator>=(ConstArrayIterator a, ConstArrayIterator b) {
    return !(a < b);
  }

 private:
  const Array* arr_;  // Array object over which we are iterating.
  size_type pos_;     // Current position within the array.
};

}  // namespace arolla

#endif  // AROLLA_UTIL_ITERATOR_H_
