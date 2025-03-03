package metrics

import (
	"strings"
)

func getMetricKey(symbol string) string {
	return strings.ToUpper(symbol)
}
