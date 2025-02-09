package svc

import (
	"errors"
	"time"
)

var (
	EmptyStorage = errors.New("empty table")
)

type TimePair struct {
	Earliest time.Time
	Latest   time.Time
}
