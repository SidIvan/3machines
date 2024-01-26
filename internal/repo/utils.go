package repo

import (
	"DeltaReceiver/internal/model"
	"github.com/ClickHouse/ch-go/proto"
)

var symbol2Enum = map[model.Symbol]proto.Enum8{
	model.BTCUSDT: 0,
}

func convSymbol2Enum(symb model.Symbol) proto.Enum8 {
	return symbol2Enum[symb]
}
