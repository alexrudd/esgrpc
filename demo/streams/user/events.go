package user

import "github.com/alexrudd/esgrpc/demo/global"

func Init() {
	global.RegisterConstructor(func() interface{} { return &UserRegistered{} })
	global.RegisterConstructor(func() interface{} { return &UserAddedAddress{} })
	global.RegisterConstructor(func() interface{} { return &UserAddedPhoneNumber{} })
	global.RegisterConstructor(func() interface{} { return &UserClosedAccount{} })
}

type UserRegistered struct {
	Username string
	Email    string
}

type UserAddedAddress struct {
	Username string
	Address  string
}

type UserAddedPhoneNumber struct {
	Username    string
	PhoneNumber string
}

type UserClosedAccount struct {
	Username string
}
