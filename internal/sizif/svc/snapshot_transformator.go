package svc

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/log"
	"fmt"
	"sort"

	"go.uber.org/zap"
)

type DepthSnapshotTransformator struct {
	logger *zap.Logger
}

func NewDepthSnapshotTransformator() *DepthSnapshotTransformator {
	return &DepthSnapshotTransformator{
		logger: log.GetLogger("DepthSnapshotTransformator"),
	}
}

func (s DepthSnapshotTransformator) Transform(snapshotParts []model.DepthSnapshotPart, key *model.ProcessingKey) ([][]model.DepthSnapshotPart, bool) {
	if len(snapshotParts) == 0 {
		s.logger.Warn(fmt.Sprintf("empty batch for key %s", key))
		return nil, false
	}
	minAllowedTsMs := key.HourNo * millisInHour
	maxAllowedTsMs := minAllowedTsMs + millisInHour - 1
	snapshotPartsOutsideTimeRange := 0
	for _, snapshotPart := range snapshotParts {
		if snapshotPart.Timestamp > maxAllowedTsMs || snapshotPart.Timestamp < minAllowedTsMs {
			snapshotPartsOutsideTimeRange++
		}
	}
	validByTimeRange := true
	if snapshotPartsOutsideTimeRange > 0 {
		s.logger.Warn(fmt.Sprintf("invalid time range for %s key", key))
		validByTimeRange = false
	}
	tsToSnapshot := make(map[int64][]model.DepthSnapshotPart)
	for _, snapshotPart := range snapshotParts {
		if snapshot, ok := tsToSnapshot[snapshotPart.Timestamp]; ok {
			snapshot = append(snapshot, snapshotPart)
		} else {
			tsToSnapshot[snapshotPart.Timestamp] = []model.DepthSnapshotPart{snapshotPart}
		}
	}
	s.logger.Info(fmt.Sprintf("got %d different snapshots", len(tsToSnapshot)))
	transformedSnaphots := make([][]model.DepthSnapshotPart, len(tsToSnapshot))
	for i, snapshot := range tsToSnapshot {
		sort.Slice(snapshot, func(i, j int) bool {
			return (snapshot[i].T && !snapshot[j].T) || snapshot[i].Price < snapshot[j].Price
		})
		transformedSnaphots[i] = snapshot
	}
	return transformedSnaphots, validByTimeRange
}
