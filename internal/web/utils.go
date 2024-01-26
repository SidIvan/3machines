package web

import (
	"DeltaReceiver/internal/model"
	bmodel "DeltaReceiver/pkg/binance/model"
)

func convBinanceSymb2Symb(symb bmodel.Symbol) model.Symbol {
	return model.SymbolFromString(string(symb))
}
