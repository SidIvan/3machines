package model

type Symbol string

const (
	BTCUSDT       = Symbol("btcusdt")
	BTCFDUSD      = Symbol("btcfdusd")
	JUPUSDT       = Symbol("jupusdt")
	_1000SATSUSDT = Symbol("1000satsusdt")
	SOLUSDT       = Symbol("solusdt")
	ETHFDUSD      = Symbol("ethfdusd")
	SEIUSDT       = Symbol("seiusdt")
	AVAXUSDT      = Symbol("avaxusdt")
	TIAUSDT       = Symbol("tiausdt")
	RUNEUSDT      = Symbol("runeusdt")
	FTTUSDT       = Symbol("fttusdt")
	RNDRUSDT      = Symbol("rndrusdt")
	FDUSDUSDT     = Symbol("fdusdusdt")
	USDCUSDT      = Symbol("usdcusdt")
	XMRBTC        = Symbol("xmrbtc")
	TRXTRY        = Symbol("trxtry")
	ARKMUSDT      = Symbol("arkmusdt")
	PEPETRY       = Symbol("pepetry")
	BONDUSDT      = Symbol("bondusdt")
	DCRUSDT       = Symbol("dcrusdt")
	LINKETH       = Symbol("linketh")
	RARETRY       = Symbol("raretry")
	BELUSDT       = Symbol("belusdt")
	BALUSDT       = Symbol("balusdt")
	BTCPLN        = Symbol("btcpln")
	MANTABNB      = Symbol("mantabnb")
	DEXEUSDT      = Symbol("dexeusdt")
	SCRTUSDT      = Symbol("scrtusdt")
	CHZBTC        = Symbol("chzbtc")
	LPTBNB        = Symbol("lptbnb")
	LOKABTC       = Symbol("lokabtc")
	CHESSBTC      = Symbol("chessbtc")
	BNBUPUSDT     = Symbol("bnbupusdt")
	GRTEUR        = Symbol("grteur")
)

var string2Symb = map[string]Symbol{
	"btcusdt":      BTCUSDT,
	"btcfdusd":     BTCFDUSD,
	"jupusdt":      JUPUSDT,
	"1000satsusdt": _1000SATSUSDT,
	"solusdt":      SOLUSDT,
	"ethfdusd":     ETHFDUSD,
	"seiusdt":      SEIUSDT,
	"avaxusdt":     AVAXUSDT,
	"tiausdt":      TIAUSDT,
	"runeusdt":     RUNEUSDT,
	"fttusdt":      FTTUSDT,
	"rndrusdt":     RNDRUSDT,
	"fdusdusdt":    FDUSDUSDT,
	"usdcusdt":     USDCUSDT,
	"xmrbtc":       XMRBTC,
	"trxtry":       TRXTRY,
	"arkmusdt":     ARKMUSDT,
	"pepetry":      PEPETRY,
	"bondusdt":     BONDUSDT,
	"dcrusdt":      DCRUSDT,
	"linketh":      LINKETH,
	"raretry":      RARETRY,
	"belusdt":      BELUSDT,
	"balusdt":      BALUSDT,
	"btcpln":       BTCPLN,
	"mantabnb":     MANTABNB,
	"dexeusdt":     DEXEUSDT,
	"scrtusdt":     SCRTUSDT,
	"chzbtc":       CHZBTC,
	"lptbnb":       LPTBNB,
	"lokabtc":      LOKABTC,
	"chessbtc":     CHESSBTC,
	"bnbupusdt":    BNBUPUSDT,
	"grteur":       GRTEUR,
}

func SymbolFromString(symb string) Symbol {
	return string2Symb[symb]
}
