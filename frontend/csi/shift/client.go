// Copyright 2025 NetApp, Inc. All Rights Reserved.

package shift

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	. "github.com/netapp/trident/logging"
	"github.com/netapp/trident/storage"
)

// Shift service in-cluster endpoint.
// The service is a NodePort service named "shift-service" in namespace "shift".
// From inside the cluster we use the ClusterIP DNS name directly.
const ShiftServiceURL = "https://shift-service.shift.svc.cluster.local:3704/api/recovery/conversion-pvc"

// Shift API numeric status codes
const (
	StatusRunning = 3
	StatusSuccess = 4
	StatusFailed  = 5
)

// JobStatus represents the logical status of a Shift job.
type JobStatus string

const (
	JobStatusSuccess JobStatus = "success"
	JobStatusRunning JobStatus = "running"
	JobStatusFailed  JobStatus = "failed"
)

// --- Shift API request payload ---

type OntapCredentials struct {
	EndPoint          string `json:"endPoint"`
	LoginID           string `json:"loginId"`
	Password          string `json:"password"`
	SkipSSLValidation bool   `json:"skipSSLValidation"`
}

type PVCRef struct {
	UID  string `json:"uid"`
	Name string `json:"name"`
}

type ShiftOptions struct {
	TimeoutSecs int `json:"timeoutSecs"`
}

type Request struct {
	NFSSharePath       string           `json:"nfsSharePath"`
	SVMName            string           `json:"svmName"`
	DiskFileName       string           `json:"diskFileName"`
	DatastoreRemotePath string          `json:"datastoreRemotePath"`
	OntapCredentials   OntapCredentials `json:"ontapCredentials"`
	PVC                PVCRef           `json:"pvc"`
	PlanType           string           `json:"planType"`
	Options            ShiftOptions     `json:"options"`
}

// --- Shift API response payload ---

type Response struct {
	ID               string  `json:"id"`
	InternalID       string  `json:"_id"`
	Href             string  `json:"href"`
	Status           int     `json:"status"`
	ClonedVolumeName *string `json:"clonedVolumeName"`
	Message          string  `json:"message"`
}

func (r *Response) JobStatus() JobStatus {
	switch r.Status {
	case StatusSuccess:
		return JobStatusSuccess
	case StatusRunning:
		return JobStatusRunning
	case StatusFailed:
		return JobStatusFailed
	default:
		if r.Href != "" && r.Status == 0 {
			return JobStatusRunning
		}
		return JobStatusFailed
	}
}

func (r *Response) VolumeName() string {
	if r.ClonedVolumeName != nil {
		return *r.ClonedVolumeName
	}
	return ""
}

// Client is the interface for invoking Shift jobs.
type Client interface {
	InvokeShiftJob(ctx context.Context, shiftCfg *storage.ShiftConfig) (*Response, error)
}

// --- Real HTTP client ---

type httpClient struct {
	client *http.Client
}

func NewClient() Client {
	return &httpClient{
		client: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // #nosec G402
			},
		},
	}
}

func (c *httpClient) InvokeShiftJob(
	ctx context.Context, shiftCfg *storage.ShiftConfig,
) (*Response, error) {

	reqBody := &Request{
		NFSSharePath:        shiftCfg.NFSPath,
		SVMName:             shiftCfg.SVM,
		DiskFileName:        shiftCfg.DiskPath,
		DatastoreRemotePath: shiftCfg.NFSPath,
		OntapCredentials: OntapCredentials{
			EndPoint:          shiftCfg.ManagementLIF,
			LoginID:           shiftCfg.Username,
			Password:          shiftCfg.Password,
			SkipSSLValidation: true,
		},
		PVC: PVCRef{
			UID:  shiftCfg.PVCUID,
			Name: shiftCfg.PVCName,
		},
		PlanType: "openshift-mtv",
		Options: ShiftOptions{
			TimeoutSecs: 3600,
		},
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("shift: failed to marshal request: %v", err)
	}

	Logc(ctx).WithFields(LogFields{
		"endpoint":      ShiftServiceURL,
		"pvcUID":        shiftCfg.PVCUID,
		"svm":           shiftCfg.SVM,
		"managementLIF": shiftCfg.ManagementLIF,
		"nfsPath":       shiftCfg.NFSPath,
		"diskPath":      shiftCfg.DiskPath,
	}).Info("Shift: invoking Shift REST endpoint.")

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ShiftServiceURL, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("shift: failed to create HTTP request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	if shiftCfg.ShiftAPIUsername == "" || shiftCfg.ShiftAPIPassword == "" {
		return nil, fmt.Errorf("shift: missing API credentials; ensure the shift-credentials secret exists in the shift namespace")
	}
	httpReq.SetBasicAuth(shiftCfg.ShiftAPIUsername, shiftCfg.ShiftAPIPassword)

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("shift: HTTP request failed: %v", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("shift: failed to read response body: %v", err)
	}

	Logc(ctx).WithFields(LogFields{
		"httpStatus":   httpResp.StatusCode,
		"responseBody": string(body),
	}).Info("Shift: received response from Shift endpoint.")

	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, fmt.Errorf("shift: endpoint returned HTTP %d: %s", httpResp.StatusCode, string(body))
	}

	var resp Response
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("shift: failed to unmarshal response: %v", err)
	}

	Logc(ctx).WithFields(LogFields{
		"status":           resp.Status,
		"jobStatus":        resp.JobStatus(),
		"clonedVolumeName": resp.VolumeName(),
		"id":               resp.ID,
		"internalID":       resp.InternalID,
		"href":             resp.Href,
		"message":          resp.Message,
	}).Info("Shift: parsed Shift response.")

	return &resp, nil
}
