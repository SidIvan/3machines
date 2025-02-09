package model

type WithTimestampMs interface {
	GetTimestampMs() int64
}

type WithSymbol interface {
	GetSymbol() string
}

type BinanceDataRow interface {
	WithTimestampMs
	WithSymbol
}
