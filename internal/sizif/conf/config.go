package conf

import (
	"DeltaReceiver/internal/common/conf"
	cconf "DeltaReceiver/pkg/conf"
)

type AppConfig struct {
	ZkCfg          *ZkConfig            `yaml:"zk"`
	B2Cfg          *B2Config            `yaml:"b2"`
	DwarfURIConfig *cconf.BaseUriConfig `yaml:"dwarf.uri"`
	SocratesCfg    *conf.CsRepoConfig   `yaml:"socrates"`
	BinanceSpotCfg *BinanceMarketCfg    `yaml:"binance.spot"`
	BinanceUSDCfg  *BinanceMarketCfg    `yaml:"binance.usd"`
	BinanceCoinCfg *BinanceMarketCfg    `yaml:"binance.coin"`
}

func AppConfigFromEnv(prefix string) *AppConfig {
	return &AppConfig{
		ZkCfg:          ZkConfigFromEnv("zk"),
		B2Cfg:          B2ConfigFromEnv("b2"),
		SocratesCfg:    conf.NewCsRepoConfigFromEnv("socrates"),
		DwarfURIConfig: cconf.NewBaseUriConfigFromEnv("dwarf.uri"),
		BinanceSpotCfg: NewBinanceMarketCfg("binance.spot"),
		BinanceUSDCfg:  NewBinanceMarketCfg("binance.usd"),
		BinanceCoinCfg: NewBinanceMarketCfg("binance.coin"),
	}
}
