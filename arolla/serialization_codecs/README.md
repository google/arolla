# About serialization format

The serialization format represents a decoding instruction of a sequence of
entities. An entity can be either a value or an expression. Decoding of the next
entity may depend on the previously decoded entities in the sequence.

Example:

*   ...
*   n-3: decode_operator
*   n-2: decode_expr_1
*   n-1: decode_expr_2
*   n: construct a node using an operator from step n-3, and dependencies from
    steps n-2 and n-1
*   ...

Codecs control serialization format of values. And the serialization format for
expression nodes is predefined and cannot be changed.

Each value gets serialized to a ValueProto message (see
serialization_base/base.proto):

```
message ValueProto {
  // References to past decoding steps with values.
  repeated int64 input_value_indices = 1;
  // References to past decoding steps with expressions.
  repeated int64 input_expr_indices = 2;

  // Index of a codec from `ContainerProto.codecs`.
  optional int64 codec_index = 3;
  extensions 10000 to max;
}
```

`input_value_indices` and `input_expr_indices` represent references to the
previous steps with dependencies needed for decoding this value. It's expected
that each codec has a corresponding
[Protobuf Message Extension](https://developers.google.com/protocol-buffers/docs/proto#extensions),
that may also store extra data.

Example from `serialization_codecs/generic/scalar_codec.proto`:

```
message ScalarV1Proto {
  extend arolla.serialization_base.ValueProto {
    optional ScalarV1Proto extension = 337082660;
  }
  oneof value {
    bool unit_value = 1;
    bool boolean_value = 2;
    bytes bytes_value = 3;
    string text_value = 4;
    ...
  }
}
```

A more advanced example from
`serialization_codecs/generic/operator_codec.proto`:

```
message OperatorV1Proto {
  extend arolla.serialization_base.ValueProto {
    optional OperatorV1Proto extension = 326031909;
  }

  // Representes LambdaOperator.
  //
  // ValueProto.input_value_indices
  //   -- Default values for operator signature.
  //
  // ValueProto.input_expr_indices[0]
  //   -- Lambda body expression.
  //
  message LambdaOperatorProto {
    optional bytes name = 1;
    optional bytes signature_spec = 2;
  }


  oneof value {
    // Represents arolla::expr::RegisteredOperator.
    //
    // This fields stores name of an operator from the operator registry.
    bytes registered_operator_name = 1;

    // Represents arolla::expr::LambdaOperator.
    LambdaOperatorProto lambda_operator = 2;
    ...
  }
```

Here the text `signature_spec` is represented by
`ValueProto.Extensions[OperatorV1Proto.extension].lambda_operator.signature_spec`
field, and the default values for the signature are represented by back
references in `ValueProto.input_value_indices`.

# How to add serialization for a new value type?

1.  We expect that all new codecs and value types should have exhaustive
    unit-tests, including the tests for decoder error messages.

    At this moment, most of tests are implemented in Python. Please check
    directory: py/arolla/s11n/testing

2.  If you want to support a new value type, you should either create a new
    codec or extend an existing one. Modifying an existing codec is preferable
    if the new type is similar to one of the existing. For example, UINT32 could
    be serialized with scalars and OPTIONAL_UINT32 with optionals.

    Another factor to consider is the binary size and modularity. For example,
    it might be more desirable to have a separate codec for ForestModelOperator,
    so clients who don't need decision forest evaluation and who care about the
    binary size, could disable it.

3.  Add a new value case to the codec's proto message.

    Even if you extend serialization for a type that was already supported, it
    is still preferable to create a new value case. By doing so, you avoid
    potential issues related to the partial message deserialization, when a new
    encoder adds a field, that old decoders ignore.

    NOTE: When you implement serialization for a new value type, please also
    implement serialization for the corresponding qtype. Currently we assume
    that an encoder that implements a value serialization also knows how to
    serialize value's qtype.

4.  Implement encoding/decoding in `serialization_codecs`,
    e.g. `serialization_codecs/generic/{*_coded.proto,encoder/*,decoder/*}`.

5.  Please add a new rule to `serialization/encode.cc`, and
    value-decoder registration to `py/arolla/s11n/clib_clif_aux.h`,
    if needed.

## Existing codecs

*   operator_{encoder,decoder} -- codec for EXPR operator serialization (like
    RegisteredOperator or LambdaOperator; cl/337047607)
*   optional_{encoder,decoder} -- codec for optional values and corresponding
    qtypes (cl/337330520)
*   scalar_{encoder,decoder} -- codec for scalar values and corresponding qtypes
    (cl/337269038)
*   tuple_{encoder,decoder} -- codec for tuples and tuple qtypes (cl/337460656)
