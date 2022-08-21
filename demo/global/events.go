package global

import "reflect"

var constructors map[string]EventConstructor

type EventConstructor func() interface{}

func RegisterConstructor(constructor EventConstructor) {
	if constructors == nil {
		constructors = map[string]EventConstructor{}
	}

	constructors[reflect.TypeOf(constructor()).Name()] = constructor
}
