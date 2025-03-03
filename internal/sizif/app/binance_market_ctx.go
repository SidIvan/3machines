package app

import (
	cconf "DeltaReceiver/internal/common/conf"
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/common/repo/cs"
	"DeltaReceiver/internal/common/web"
	b2pqt "DeltaReceiver/internal/sizif/b2"
	"DeltaReceiver/internal/sizif/conf"
	"DeltaReceiver/internal/sizif/lock"
	"DeltaReceiver/internal/sizif/metrics"
	"DeltaReceiver/internal/sizif/svc"
	bmodel "DeltaReceiver/pkg/binance/model"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/Backblaze/blazer/b2"
	"github.com/go-zookeeper/zk"
	"github.com/gocql/gocql"
)

type BinanceMarketCtx struct {
	deltasSvc    *svc.SizifSvc[model.Delta]
	bookTicksSvc *svc.SizifSvc[bmodel.SymbolTick]
	snapshotsSvc *svc.SizifSvc[model.DepthSnapshotPart]
}

func NewBinanceMarketCtx(
	marketType bmodel.DataType,
	marketCfg *conf.BinanceMarketCfg,
	csRepoCfg *cconf.BinanceMarketCsRepoCfg,
	zkConn *zk.Conn,
	b2Bucket *b2.Bucket,
	csSession *gocql.Session,
	dwarfClient *web.DwarfHttpClient,
) *BinanceMarketCtx {
	var marketSubpath string
	if marketType == bmodel.Spot {
		marketSubpath = "binance/"
	} else {
		marketSubpath = fmt.Sprintf("binance/%s/", marketType)
	}

	deltaSubpath := marketSubpath + "deltas"
	deltaSocratesStorage := cs.NewCsDeltaStorageRO(csSession, csRepoCfg.DeltaTableName, csRepoCfg.DeltaKeyTableName)
	deltaParquetStorage := b2pqt.NewB2ParquetStorage[model.Delta](b2Bucket, deltaSubpath, b2pqt.FromKey)
	deltaTransformator := svc.NewDeltaTransformator(dwarfClient)
	deltaLocker := lock.NewZkLocker(deltaSubpath, zkConn)
	deltaMetrics := metrics.NewSizifWorkerMetrics(b2zkPathToMetric(deltaSubpath))
	deltaSvc := svc.NewSizifSvc(deltaSubpath, deltaSocratesStorage, deltaParquetStorage, deltaTransformator, deltaLocker, marketCfg.DeltaWorkers, deltaMetrics)

	bookTicksSubpath := marketSubpath + "book_ticks"
	bookTicksSocratesStorage := cs.NewCsBookTicksStorageRO(csSession, csRepoCfg.BookTicksTableName, csRepoCfg.BookTicksKeyTableName)
	bookTicksParquetStorage := b2pqt.NewB2ParquetStorage[bmodel.SymbolTick](b2Bucket, bookTicksSubpath, b2pqt.FromKey)
	bookTicksTransformator := svc.NewBookTicksTransformator()
	bookTicksLocker := lock.NewZkLocker(bookTicksSubpath, zkConn)
	bookTicksMetrics := metrics.NewSizifWorkerMetrics(b2zkPathToMetric(bookTicksSubpath))
	bookTicksSvc := svc.NewSizifSvc(bookTicksSubpath, bookTicksSocratesStorage, bookTicksParquetStorage, bookTicksTransformator, bookTicksLocker, marketCfg.BookTicksWorker, bookTicksMetrics)

	snapshotsSubpath := marketSubpath + "snapshots"
	snapshotsSocratesStorage := cs.NewCsSnapshotStorageRO(csSession, csRepoCfg.SnapshotTableName, csRepoCfg.SnapshotKeyTableName)
	snapshotsParquetStorage := b2pqt.NewB2ParquetStorage[model.DepthSnapshotPart](b2Bucket, snapshotsSubpath, b2pqt.FromData)
	snapshotsTransformator := svc.NewDepthSnapshotTransformator()
	snapshotsLocker := lock.NewZkLocker(snapshotsSubpath, zkConn)
	snapshotsMetrics := metrics.NewSizifWorkerMetrics(b2zkPathToMetric(snapshotsSubpath))
	snapshotsSvc := svc.NewSizifSvc(snapshotsSubpath, snapshotsSocratesStorage, snapshotsParquetStorage, snapshotsTransformator, snapshotsLocker, marketCfg.SnapshotsWorker, snapshotsMetrics)

	return &BinanceMarketCtx{
		deltasSvc:    deltaSvc,
		bookTicksSvc: bookTicksSvc,
		snapshotsSvc: snapshotsSvc,
	}
}

func b2zkPathToMetric(path string) string {
	return strings.Replace(path, "/", "_", -1)
}

func (s *BinanceMarketCtx) Start(ctx context.Context) {
	go s.deltasSvc.Start(ctx)
	go s.bookTicksSvc.Start(ctx)
	go s.snapshotsSvc.Start(ctx)
}

func (s *BinanceMarketCtx) Shutdown(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		s.deltasSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.bookTicksSvc.Shutdown(ctx)
		wg.Done()
	}()
	go func() {
		s.snapshotsSvc.Shutdown(ctx)
		wg.Done()
	}()
	wg.Wait()
}
