package Err

import "errors"

const (
	SourceNotExist        string = "ErrSourceNotExist"
	Failure               string = "ErrFailure"
	SourceAlreadyExist    string = "ErrSourceAlreadyExist"
	SourceNotEnough       string = "ErrSourceNotEnough"
	RequestIllegal        string = "ErrRequestIllegal"
	RequestTimeout        string = "ErrRequestTimeout"
	RequestServerNotServe string = "ErrRequestServerNotServe"
	RequestNotLeader      string = "ErrRequestNotLeader"
	NeedToWait            string = "ErrNeedToWait"
)

var (
	ErrSourceNotExist        error = errors.New(SourceNotExist)
	ErrFailure               error = errors.New(Failure)
	ErrSourceAlreadyExist    error = errors.New(SourceAlreadyExist)
	ErrSourceNotEnough       error = errors.New(SourceNotEnough)
	ErrRequestIllegal        error = errors.New(RequestIllegal)
	ErrRequestTimeout        error = errors.New(RequestTimeout)
	ErrRequestServerNotServe error = errors.New(RequestServerNotServe)
	ErrRequestNotLeader      error = errors.New(RequestNotLeader)
	ErrNeedToWait            error = errors.New(NeedToWait)
)
