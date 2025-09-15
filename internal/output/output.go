package output

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type OutputFormat struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func Success(message string, args ...interface{}) {
	fmt.Printf("guppy: "+message+"\n", args...)
}

func Error(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
}

func Warning(message string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "warning: "+message+"\n", args...)
}

func JSON(data interface{}) error {
	return json.NewEncoder(os.Stdout).Encode(OutputFormat{
		Status: "success",
		Data:   data,
	})
}

func JSONError(err error) error {
	return json.NewEncoder(os.Stderr).Encode(OutputFormat{
		Status:  "error",
		Message: err.Error(),
	})
}

func Table(data [][]string) {
	if len(data) == 0 {
		return
	}
	
	cols := len(data[0])
	widths := make([]int, cols)
	for _, row := range data {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	format := ""
	for i, width := range widths {
		if i == len(widths)-1 {
			format += "%-" + fmt.Sprintf("%d", width) + "s"
		} else {
			format += "%-" + fmt.Sprintf("%d", width) + "s  "
		}
	}
	format += "\n"

	for _, row := range data {
		fmt.Printf(format, toInterface(row)...)
	}
}

func toInterface(strs []string) []interface{} {
	intf := make([]interface{}, len(strs))
	for i, s := range strs {
		intf[i] = s
	}
	return intf
}
