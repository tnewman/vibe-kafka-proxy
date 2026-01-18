package main

import (
	"fmt"
	"reflect"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var o kgo.Offset
	fmt.Printf("Type of kgo.Offset: %v\n", reflect.TypeOf(o))
	fmt.Printf("Underlying kind of kgo.Offset: %v\n", reflect.TypeOf(o).Kind())
	fmt.Printf("Fields of kgo.Offset (if struct): %v\n", reflect.TypeOf(o).NumField())
	for i := 0; i < reflect.TypeOf(o).NumField(); i++ {
		field := reflect.TypeOf(o).Field(i)
		fmt.Printf("  Field %d: Name=%s, Type=%v, PkgPath=%s\n", i, field.Name, field.Type, field.PkgPath)
	}
}
