package common

import (
	"reflect"
)

var ERpcCmd = RpcCmd{}

// JobStatus indicates the status of a Job; the default is InProgress.
type RpcCmd EnumString

func (RpcCmd) CopyJobPartOrder() RpcCmd { return RpcCmd{"CopyJobPartOrder"} }
func (RpcCmd) ListJobs() RpcCmd         { return RpcCmd{"ListJobs"} }
func (RpcCmd) ListJobSummary() RpcCmd   { return RpcCmd{"ListJobSummary"} }
func (RpcCmd) ListJobTransfers() RpcCmd { return RpcCmd{Value: "ListJobTransfers"} }
func (RpcCmd) CancelJob() RpcCmd        { return RpcCmd{"CancelJob"} }
func (RpcCmd) PauseJob() RpcCmd         { return RpcCmd{"PauseJob"} }
func (RpcCmd) ResumeJob() RpcCmd        { return RpcCmd{"ResumeJob"} }

func (c RpcCmd) String() string {
	return EnumString(c).String(reflect.TypeOf(c))
}
func (c RpcCmd) Pattern() string { return "/" + c.String() }

func (c RpcCmd) Parse(s string) (RpcCmd, error) {
	e, err := EnumString{}.Parse(reflect.TypeOf(c), s, true)
	return RpcCmd(e), err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// This struct represents the job info (a single part) to be sent to the storage engine
type CopyJobPartOrderRequest struct {
	Version            uint32     // version of the azcopy
	JobID              JobID      // Guid - job identifier
	PartNum            PartNumber // part number of the job
	IsFinalPart        bool       // to determine the final part for a specific job
	Priority           uint8      // priority of the task
	SrcLocation        Location
	DstLocation        Location
	Transfers          []CopyTransfer
	LogLevel           LogLevel
	IsaBackgroundOp    bool
	OptionalAttributes BlobTransferAttributes
}

// represents the raw list command input from the user when requested the list of transfer with given status for given JobId
type ListRequest struct {
	JobID    JobID
	OfStatus string
}

// This struct represents the optional attribute for blob request header
type BlobTransferAttributes struct {
	BlobType                 BlobType // The type of a blob - BlockBlob, PageBlob, AppendBlob
	ContentType              string   //The content type specified for the blob.
	ContentEncoding          string   //Specifies which content encodings have been applied to the blob.
	Metadata                 string   //User-defined name-value pairs associated with the blob
	NoGuessMimeType          bool     // represents user decision to interpret the content-encoding from source file
	PreserveLastModifiedTime bool     // when downloading, tell engine to set file's timestamp to timestamp of blob
	BlockSizeinBytes         uint32
}

// ListJobsResponse represent the Job with JobId and
type ListJobsResponse struct {
	ErrorMessage string
	JobIDs       []JobID
}

// represents the JobProgress Summary response for list command when requested the Job Progress Summary for given JobId
type ListJobSummaryResponse struct {
	ErrorMsg string
	JobID    JobID
	// CompleteJobOrdered determines whether the Job has been completely ordered or not
	CompleteJobOrdered             bool
	JobStatus                      JobStatus
	TotalNumberOfTransfers         uint32
	TotalNumberofTransferCompleted uint32
	TotalNumberofFailedTransfer    uint32
	//NumberOfTransferCompletedafterCheckpoint uint32
	//NumberOfTransferFailedAfterCheckpoint    uint32
	FailedTransfers             []TransferDetail
	ThroughputInBytesPerSeconds float64
}

// represents the Details and details of a single transfer
type TransferDetail struct {
	Src            string
	Dst            string
	TransferStatus TransferStatus
}

type CancelPauseResumeResponse struct {
	ErrorMsg              string
	CancelledPauseResumed bool
}

type CopyJobPartOrderResponse struct {
	ErrorMsg   string
	JobStarted bool
}

type FooResponse struct { // TODO: What is this?
	ErrorMsg string
}

// represents the list of Details and details of number of transfers
type ListJobTransfersResponse struct {
	ErrorMsg string
	JobID    JobID
	Details  []TransferDetail
}