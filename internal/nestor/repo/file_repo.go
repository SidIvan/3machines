package repo

import (
	"DeltaReceiver/pkg/log"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
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
	dirPath := fmt.Sprintf("/var/data/%s", dataType)
	logger := log.GetLogger(fmt.Sprintf("FileRepo[%s]", dataType))
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		logger.Error(err.Error())
	}
	return &FileRepo[T]{
		dirPath: dirPath,
		logger:  logger,
	}
}

func (s *FileRepo[T]) Save(ctx context.Context, batch []T) error {
	file, err := os.Create(fmt.Sprintf("%s/%d_%d_%d", s.dirPath, time.Now().UnixMilli(), rand.Int64(), rand.Int64()))
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
			s.logger.Info(fmt.Sprintf("working with file %s", filePath))
			rawData, err := os.ReadFile(filePath)
			if err != nil {
				s.logger.Error(fmt.Errorf("error opening file %s: %w", filePath, err).Error())
				return nil, err, func() error { return nil }
			}
			var data []T
			err = json.Unmarshal(rawData, &data)
			if err != nil {
				s.logger.Error(fmt.Errorf("error unmarshaling data %s: %w", filePath, err).Error())
				return nil, err, func() error { return nil }
			}
			return data, nil, func() error {
				err := os.Remove(filePath)
				if err == nil {
					s.logger.Info(fmt.Sprintf("file %s deleted", filePath))
				}
				return err
			}
		}
	}
	return nil, nil, func() error { return nil }
}
