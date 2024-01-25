package env

import "os"

type EnvType string

const (
	DEV  = EnvType("dev")
	PROD = EnvType("prod")
	TEST = EnvType("test")
)

func GetEnvType() EnvType {
	envType := EnvType(os.Getenv("env"))
	if envType == DEV {
		return DEV
	}
	if envType == PROD {
		return PROD
	}
	if envType == TEST {
		return TEST
	}
	return DEV
}
