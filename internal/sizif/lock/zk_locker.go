package lock

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/internal/sizif/svc"
	"DeltaReceiver/pkg/log"
	"context"
	"fmt"
	"strings"
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
	ttl       time.Duration = time.Hour * 24 * 7
)

func (s ZkLocker) Lock(ctx context.Context, key *model.ProcessingKey) (svc.LockOpStatus, error) {
	lockPath := s.createZkPath(key)
	_, err := s.conn.CreateTTL(lockPath, locked, zk.FlagTTL, zk.WorldACL(zk.PermAll), ttl)
	if err != nil {
		if err == zk.ErrNoNode {
			lockSubPaths := strings.Split(lockPath, "/")
			nodePath := ""
			for i := 1; i < len(lockSubPaths)-1; i++ {
				nodePath += "/" + lockSubPaths[i]
				exists, _, err := s.conn.Exists(nodePath)
				if err != nil {
					return svc.AlreadyLocked, err
				}
				if !exists {
					_, err = s.conn.Create(nodePath, []byte{}, zk.FlagPersistent, zk.WorldACL(zk.PermAll))
					if err != nil {
						s.logger.Error(err.Error())
						return svc.AlreadyLocked, err
					}
				}
			}
			return s.Lock(ctx, key)
		} else if err == zk.ErrNodeExists {
			if status, _, err := s.conn.Get(lockPath); err != nil {
				s.logger.Error(err.Error())
				return svc.AlreadyLocked, err
			} else if status[0] == processed[0] {
				return svc.AlreadyProcessed, nil
			}
			return svc.AlreadyLocked, nil
		} else {
			s.logger.Error(err.Error())
			return svc.AlreadyLocked, err
		}
	}
	s.logger.Info(fmt.Sprintf("lock node created %s", lockPath))
	return svc.LockedSuccessfully, nil
}

func (s ZkLocker) Unlock(ctx context.Context, key *model.ProcessingKey) error {
	lockPath := s.createZkPath(key)
	var err error
	for i := 0; i < 3; i++ {
		err := s.conn.Delete(lockPath, 0)
		if err == nil {
			return nil
		}
		if err == zk.ErrNoNode {
			s.logger.Warn(fmt.Sprintf("no lock node for path %s", lockPath))
			return nil
		}
		s.logger.Error(err.Error())
	}
	return err
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
