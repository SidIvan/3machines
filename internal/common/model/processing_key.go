package model

import (
	"fmt"
	"time"
)

type ProcessingKey struct {
	Symbol string `cql:"symbol"`
	HourNo int64  `cql:"hour"`
}

func (s *ProcessingKey) GetStartTime() time.Time {
	return time.UnixMilli(s.HourNo * 60 * 60 * 1000)
}

func (s *ProcessingKey) GetEndTime() time.Time {
	return time.UnixMilli((s.HourNo + 1) * 60 * 60 * 1000)
}

func (s *ProcessingKey) String() string {
	return fmt.Sprintf("[%s, %s, %s]", s.Symbol, s.GetStartTime(), s.GetEndTime())
}
