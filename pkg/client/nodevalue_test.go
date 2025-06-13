package client_test

import (
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-ucanto/testing/helpers"
	"github.com/storacha/guppy/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestNodeValue(t *testing.T) {
	ts := new(schema.TypeSystem)
	ts.Init()
	schema.SpawnDefaultBasicTypes(ts)

	t.Run("returns nil for absent node", func(t *testing.T) {
		require.Nil(t, helpers.Must(client.NodeValue(datamodel.Absent)))
	})

	t.Run("returns nil for null node", func(t *testing.T) {
		require.Nil(t, helpers.Must(client.NodeValue(datamodel.Null)))
	})

	t.Run("returns the value of a bool node", func(t *testing.T) {
		value := true
		node := bindnode.Wrap(&value, ts.TypeByName("Bool"))
		require.Equal(t, value, helpers.Must(client.NodeValue(node)))
	})

	t.Run("returns the value of an int node", func(t *testing.T) {
		value := int64(42)
		node := bindnode.Wrap(&value, ts.TypeByName("Int"))
		require.Equal(t, value, helpers.Must(client.NodeValue(node)))
	})

	t.Run("returns the value of a float node", func(t *testing.T) {
		value := float64(3.14)
		node := bindnode.Wrap(&value, ts.TypeByName("Float"))
		require.Equal(t, value, helpers.Must(client.NodeValue(node)))
	})

	t.Run("returns the value of a string node", func(t *testing.T) {
		value := "hello world"
		node := bindnode.Wrap(&value, ts.TypeByName("String"))
		require.Equal(t, value, helpers.Must(client.NodeValue(node)))
	})

	t.Run("returns the value of a bytes node", func(t *testing.T) {
		value := []byte{0xde, 0xad, 0xbe, 0xef}
		node := bindnode.Wrap(&value, ts.TypeByName("Bytes"))
		require.Equal(t, value, helpers.Must(client.NodeValue(node)))
	})

	t.Run("returns the value of a link node", func(t *testing.T) {
		value := helpers.RandomCID()
		node := bindnode.Wrap(&value, ts.TypeByName("Link"))
		require.Equal(t, value, helpers.Must(client.NodeValue(node)))
	})

	t.Run("returns the value of a list node", func(t *testing.T) {
		boolValue := true
		intValue := int64(42)
		floatValue := float64(3.14)
		stringValue := "hello world"

		boolNode := bindnode.Wrap(&boolValue, ts.TypeByName("Bool"))
		intNode := bindnode.Wrap(&intValue, ts.TypeByName("Int"))
		floatNode := bindnode.Wrap(&floatValue, ts.TypeByName("Float"))
		stringNode := bindnode.Wrap(&stringValue, ts.TypeByName("String"))

		value := []ipld.Node{boolNode, intNode, floatNode, stringNode}
		node := bindnode.Wrap(&value, ts.TypeByName("List"))

		require.Equal(
			t,
			[]any{boolValue, intValue, floatValue, stringValue},
			helpers.Must(client.NodeValue(node)),
		)
	})

	t.Run("returns the value of a map node", func(t *testing.T) {
		boolValue := true
		intValue := int64(42)
		floatValue := float64(3.14)
		stringValue := "hello world"

		boolNode := bindnode.Wrap(&boolValue, ts.TypeByName("Bool"))
		intNode := bindnode.Wrap(&intValue, ts.TypeByName("Int"))
		floatNode := bindnode.Wrap(&floatValue, ts.TypeByName("Float"))
		stringNode := bindnode.Wrap(&stringValue, ts.TypeByName("String"))

		value := struct {
			Keys   []string
			Values map[string]ipld.Node
		}{
			Keys: []string{"bool", "int", "float", "string"},
			Values: map[string]ipld.Node{
				"bool":   boolNode,
				"int":    intNode,
				"float":  floatNode,
				"string": stringNode,
			},
		}
		node := bindnode.Wrap(&value, ts.TypeByName("Map"))

		require.Equal(
			t,
			map[string]any{
				"bool":   boolValue,
				"int":    intValue,
				"float":  floatValue,
				"string": stringValue,
			},
			helpers.Must(client.NodeValue(node)),
		)
	})
}
