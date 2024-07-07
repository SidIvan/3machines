package clickhouse

import (
	"DeltaReceiver/pkg/log"
	"context"
	"errors"
	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

type ChPoolHolder struct {
	logger *zap.Logger
	mut    *sync.RWMutex
	pool   *chpool.Pool
	cfg    *ChPoolCfg
}

func NewChPoolHolder(cfg *ChPoolCfg) *ChPoolHolder {
	var mut sync.RWMutex
	return &ChPoolHolder{
		logger: log.GetLogger("ChPoolHolder"),
		mut:    &mut,
		pool:   nil,
		cfg:    cfg,
	}
}

func (s *ChPoolHolder) Connect(ctx context.Context) error {
	return s.Reconnect(ctx, 1)
}

func (s *ChPoolHolder) Reconnect(ctx context.Context, numTries int) error {
	var pool *chpool.Pool
	var err error
	s.logger.Info(s.cfg.UriConf.GetAddress())
	s.logger.Info(s.cfg.User)
	s.logger.Info(s.cfg.Password)
	xd, _ := os.Hostname()
	s.logger.Info(xd)
	for i := 0; i < numTries; i++ {
		pool, err = chpool.Dial(ctx, chpool.Options{
			ClientOptions: ch.Options{
				Address:     s.cfg.UriConf.GetAddress(),
				DialTimeout: time.Duration(s.cfg.DialTimeoutS) * time.Second,
				ReadTimeout: time.Duration(s.cfg.ReadTimeoutS) * time.Second,
				User:        s.cfg.User,
				Password:    s.cfg.Password,
			},
			MaxConns: s.cfg.ChMaxConns,
			MinConns: s.cfg.ChMinConns,
		})
		if err != nil {
			continue
		}
		s.SetPool(pool)
		return nil
	}
	return err
}

func (s *ChPoolHolder) Do(ctx context.Context, query ch.Query) error {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if s.pool == nil {
		return NilConnPool
	}
	return s.pool.Do(ctx, query)
}

func (s *ChPoolHolder) SetPool(newPool *chpool.Pool) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.pool != nil {
		defer s.pool.Close()
	}
	s.pool = newPool
}

func (s *ChPoolHolder) Disconnect() {
	s.pool.Close()
}

var NilConnPool = errors.New("nil conn pool")
