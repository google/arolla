This library is intended to provide utilities for integration with protos.

# types.h

Not all C++ types from proto are used in Arolla as is. Library contains
utilities to convert types in a proto to Arolla specific C++ types.

*   `::arolla::proto::arolla_single_value_t<T>`
*   `::arolla::proto::arolla_optional_value_t<T>`
*   `::arolla::proto::arolla_array_value_t<T>`
