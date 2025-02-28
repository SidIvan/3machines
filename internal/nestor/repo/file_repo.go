package repo

import (
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

type FileRepo[T any] struct {
	logger  *zap.Logger
	dirPath string
}

func NewFileRepo[T any](dataType string) *FileRepo[T] {
	return &FileRepo[T]{
		logger:  log.GetLogger(fmt.Sprintf("FileRepo[%s]", dataType)),
		dirPath: fmt.Sprintf("/var/data/%s", dataType),
	}
}

func (s *FileRepo[T]) Save(ctx context.Context, batch []T) error {
	file, err := os.Create(fmt.Sprintf("%s/%d", s.dirPath, time.Now().UnixMilli()))
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	data, _ := json.Marshal(batch)
	if _, err = file.Write(data); err != nil {
		s.logger.Error(err.Error())
		return err
	}
	file.Close()
	return nil
}

func (s *FileRepo[T]) GetWithDeleteCallback(ctx context.Context) ([]T, error, func() error) {
	dir, err := os.ReadDir(s.dirPath)
	if err != nil {
		s.logger.Error(err.Error())
		return nil, err, func() error { return nil }
	}
	for _, dirEntry := range dir {
		if dirEntry.Type().IsRegular() {
			filePath := filepath.Join(s.dirPath, dirEntry.Name())
			rawData, err := os.ReadFile(filePath)
			if err != nil {
				s.logger.Error(fmt.Errorf("error opening file: %w", err).Error())
				return nil, err, func() error { return nil }
			}
			var data []T
			err = json.Unmarshal(rawData, &data)
			if err != nil {
				s.logger.Error(fmt.Errorf("error unmarshaling data: %w", err).Error())
				return nil, err, func() error { return nil }
			}
			return data, nil, func() error {
				return os.Remove(filePath)
			}
		}
	}
	return nil, nil, func() error { return nil }
}
