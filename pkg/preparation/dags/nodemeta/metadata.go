package nodemeta

import (
	"bytes"
	"encoding"
	"fmt"
	"reflect"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
)

type NodeMetadata[T any] struct {
	Tag   int64
	Value T
}

func Encode[T encoding.BinaryMarshaler](meta NodeMetadata[T]) ([]byte, error) {
	nb := basicnode.Prototype.List.NewBuilder()
	la, err := nb.BeginList(2)
	if err != nil {
		return nil, err
	}
	err = la.AssembleValue().AssignInt(meta.Tag)
	if err != nil {
		return nil, err
	}
	v, err := meta.Value.MarshalBinary()
	if err != nil {
		return nil, err
	}
	err = la.AssembleValue().AssignBytes(v)
	if err != nil {
		return nil, err
	}
	err = la.Finish()
	if err != nil {
		return nil, err
	}
	n := nb.Build()
	var buf bytes.Buffer
	err = dagcbor.Encode(n, &buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode[T encoding.BinaryUnmarshaler](data []byte) (NodeMetadata[T], error) {
	nb := basicnode.Prototype.List.NewBuilder()
	err := dagcbor.Decode(nb, bytes.NewReader(data))
	if err != nil {
		return NodeMetadata[T]{}, fmt.Errorf("decoding metadata: %w", err)
	}
	n := nb.Build()
	tagNode, err := n.LookupByIndex(0)
	if err != nil {
		return NodeMetadata[T]{}, fmt.Errorf("decoding metadata index 0: %w", err)
	}
	tag, err := tagNode.AsInt()
	if err != nil {
		return NodeMetadata[T]{}, fmt.Errorf("decoding metadata tag: %w", err)
	}
	valueNode, err := n.LookupByIndex(1)
	if err != nil {
		return NodeMetadata[T]{}, fmt.Errorf("decoding metadata index 1: %w", err)
	}
	valueBytes, err := valueNode.AsBytes()
	if err != nil {
		return NodeMetadata[T]{}, fmt.Errorf("decoding metadata value: %w", err)
	}
	var value T
	if reflect.TypeFor[T]().Kind() == reflect.Ptr {
		value = reflect.New(reflect.TypeFor[T]().Elem()).Interface().(T)
	}
	err = value.UnmarshalBinary(valueBytes)
	if err != nil {
		return NodeMetadata[T]{}, fmt.Errorf("decoding metadata value: %w", err)
	}
	return NodeMetadata[T]{Tag: tag, Value: value}, nil
}
