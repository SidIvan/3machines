package lock

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/sizif/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"
)

type ZkLocker struct {
	logger *zap.Logger
	prefix string
	conn   *zk.Conn
}

func NewZkLocker(prefix string, conn *zk.Conn) *ZkLocker {
	return &ZkLocker{
		logger: log.GetLogger("ZkLogger_" + prefix),
		prefix: prefix,
		conn:   conn,
	}
}

type LockType []byte

var (
	locked    LockType      = []byte("1")
	processed LockType      = []byte("2")
	ttl       time.Duration = time.Hour
)

func (s ZkLocker) Lock(ctx context.Context, key *model.ProcessingKey) (svc.LockOpStatus, error) {
	lockPath := s.createZkPath(key)
	_, err := s.conn.CreateTTL(lockPath, locked, zk.FlagTTL, zk.WorldACL(zk.PermAll), ttl)
	if err != nil {
		if err == zk.ErrNodeExists {
			return svc.AlreadyLocked, nil
		} else {
			s.logger.Error(err.Error())
			return svc.AlreadyLocked, err
		}
	}
	return svc.LockedSuccessfully, nil
}

func (s ZkLocker) MarkProcessed(ctx context.Context, key *model.ProcessingKey) error {
	lockPath := s.createZkPath(key)
	_, err := s.conn.Set(lockPath, processed, 0)
	if err != nil {
		if err == zk.ErrBadVersion {
			s.logger.Error("unexpected lock version")
		}
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

func (s *ZkLocker) createZkPath(key *model.ProcessingKey) string {
	return fmt.Sprintf("/_lock/%s/%s/%d", s.prefix, key.Symbol, key.HourNo)
}
