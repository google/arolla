# About serialization format

The serialization format represents instructions for decoding a sequence of
entities. An entity can be either a value or an expression. Decoding the next
entity may depend on previously decoded entities in the sequence.

Example:

*   ...
*   n-3: decode_operator
*   n-2: decode_expr_1
*   n-1: decode_expr_2
*   n: construct a node using an operator from step n-3, and dependencies from
    steps n-2 and n-1
*   ...

Codecs control the serialization format of values, while the serialization
format for expression nodes is predefined and unchangeable.

Each value is serialized into one or more decoding steps, with the final step
containing a ValueProto (see
arolla/serialization_base/base.proto):

```
message ValueProto {
  // References to past decoding steps with values.
  repeated uint64 input_value_indices = 1;
  // References to past decoding steps with expressions.
  repeated uint64 input_expr_indices = 2;

  // Reference to a past decoding step that declares the codec needed
  // to decode the value.
  optional uint64 codec_index = 3;

  // A range for the "unverified" allocation scheme.
  extensions 326031909 to 524999999;
}
```

`input_value_indices` and `input_expr_indices` contain references to previous
decoding steps containing dependencies needed for decoding this value.

Each codec should have a corresponding
[Protobuf Message Extension](https://developers.google.com/protocol-buffers/docs/proto#extensions)
that identifies it and can store additional data.

Example from
arolla/serialization_codecs/generic/scalar_codec.proto:

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
arolla/serialization_codecs/generic/operator_codec.proto:

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

In this case, the operator's signature specification is stored within the
`ValueProto.Extensions[OperatorV1Proto.extension].lambda_operator.signature_spec`
field. Default values for this signature are referenced via
`ValueProto.input_value_indices`.

# Notes on supporting serialization for a new value type

1.  All new codecs and value types should undergo comprehensive unit testing to
    verify functionality, prevent vulnerabilities, including specific tests to
    validate decoder error messages.

    Currently, most tests are implemented in Python. Please check the
    py/arolla/s11n/testing directory.

2.  To support a new value type, you have two options: create a new codec or
    extend an existing one. Modifying an existing codec is the preferred
    approach when the new type closely resembles one of the already supported
    types. For example, UINT32 could be serialized alongside other scalar types,
    while OPTIONAL_UINT32 would be grouped with other optional types.

    An additional consideration is the impact on binary size and modularity. For
    instance, having a dedicated codec for `ForestModelOperator` could be
    advantageous for clients who don't require decision forest evaluation and
    prioritize smaller binaries. This approach allows them to disable the
    corresponding code and reduce the overall size of their application.

3.  Even when extending serialization for an already supported type, it can be
    beneficial to create a new oneof case. This approach helps mitigate
    potential issues with partial message deserialization, particularly when a
    newer encoder version introduces fields that older decoders are not designed
    to handle.

4.  When implementing serialization for a new value type, remember to also
    implement serialization for the corresponding qtype. Currently, the
    assumption is that an encoder capable of serializing a value also knows how
    to value's qtype.
