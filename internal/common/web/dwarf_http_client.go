package web

import (
	"DeltaReceiver/internal/common/model"
	"DeltaReceiver/pkg/conf"
	"DeltaReceiver/pkg/log"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"go.uber.org/zap"
)

type DwarfHttpClient struct {
	logger      *zap.Logger
	url         string
	client      *http.Client
	serviceName string
}

func NewDwarfHttpClient(urlCfg *conf.BaseUriConfig) *DwarfHttpClient {
	serviceName := os.Getenv("SERVICE_NAME")
	logger := log.GetLogger("DwarfHttpClient")
	logger.Info(fmt.Sprintf("Got %s service name from env", serviceName))
	return &DwarfHttpClient{
		logger:      logger,
		url:         urlCfg.GetBaseUri(),
		client:      &http.Client{},
		serviceName: serviceName,
	}
}

func (s *DwarfHttpClient) SaveDeltaHole(ctx context.Context, hole model.DeltaHole) error {
	req, err := s.createRequest(hole)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Error(err.Error())
		return err
	}
	if resp.StatusCode == http.StatusInternalServerError {
		err := errors.New("500 response status from dwarf")
		s.logger.Error(err.Error())
		return err
	}
	return nil
}

func (s *DwarfHttpClient) createRequest(hole model.DeltaHole) (*http.Request, error) {
	body, err := json.Marshal(hole)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/delta/hole", s.url), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Close = true
	req.Header.Set("serviceName", s.serviceName)
	return req, nil
}
