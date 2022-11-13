package flags

import (
	flag "github.com/spf13/pflag"
	"main.go/constants"
)

var (
	baseConfigPath = flag.String(constants.BaseConfigPathKey, constants.BaseConfigPathDefaultValue, constants.BaseConfigPathUsage)
)

func BaseConfigPath() string {
	return *baseConfigPath
}
