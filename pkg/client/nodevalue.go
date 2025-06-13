package client

import (
	"fmt"

	"github.com/ipld/go-ipld-prime"
)

// NodeValue converts an arbitrary IPLD node to a Go value. This is useful as a
// last resort to see into data you don't have a specific reader for.
func NodeValue(node ipld.Node) (res any, err error) {
	if node.IsAbsent() || node.IsNull() {
		return nil, nil
	}

	switch node.Kind() {
	case ipld.Kind_Bool:
		return node.AsBool()
	case ipld.Kind_Int:
		return node.AsInt()
	case ipld.Kind_Float:
		return node.AsFloat()
	case ipld.Kind_String:
		return node.AsString()
	case ipld.Kind_Bytes:
		return node.AsBytes()
	case ipld.Kind_Link:
		return node.AsLink()
	case ipld.Kind_List:
		list := make([]any, node.Length())
		it := node.ListIterator()
		for !it.Done() {
			i, el, err := it.Next()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate list: %w", err)
			}

			item, err := NodeValue(el)
			if err != nil {
				return nil, fmt.Errorf("failed to format list item: %w", err)
			}

			list[i] = item
		}
		return list, nil
	case ipld.Kind_Map:
		m := make(map[string]any, node.Length())
		it := node.MapIterator()
		for !it.Done() {
			k, v, err := it.Next()
			if err != nil {
				return nil, fmt.Errorf("failed to iterate map: %w", err)
			}
			key, err := k.AsString()
			if err != nil {
				return nil, fmt.Errorf("failed to convert map key to string: %w", err)
			}
			value, err := NodeValue(v)
			if err != nil {
				return nil, fmt.Errorf("failed to format map value: %w", err)
			}
			m[key] = value
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported node kind: %s", node.Kind())
	}
}
