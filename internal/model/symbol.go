package model

type Symbol string

const (
	BTCUSDT = Symbol("btcusdt")
)

var string2Symb = map[string]Symbol{
	"btcusdt": BTCUSDT,
}

func SymbolFromString(symb string) Symbol {
	return string2Symb[symb]
}
