package test

// S3 error code
const (
	StatusOK                     = 200
	AccessDenied                 = 403
	BadDigest                    = 400
	BucketAlreadyExists          = 409
	BucketNotEmpty               = 409
	IncompleteBody               = 400
	InternalError                = 500
	InvalidArgument              = 400
	InvalidBucketName            = 400
	InvalidDigest                = 400
	InvalidLocationConstraint    = 400
	InvalidPart                  = 400
	InvalidPartOrder             = 400
	InvalidRange                 = 416
	InvalidRequest               = 400
	InvalidURI                   = 400
	KeyTooLong                   = 400
	MalformedACLError            = 400
	MalformedPOSTRequest         = 400
	MalformedXML                 = 400
	MaxMessageLengthExceeded     = 400
	MetadataTooLarge             = 400
	MethodNotAllowed             = 405
	MissingContentLength         = 411
	MissingRequestBodyError      = 400
	NoSuchBucket                 = 404
	NoSuchKey                    = 404
	NoSuchLifecycleConfiguration = 404
	NoSuchUpload                 = 404
	OperationAborted             = 409
	RequestTimeout               = 400
	RequestTimeTooSkewed         = 403
	SignatureDoesNotMatch        = 403
	ServiceUnavailable           = 503
	SlowDown                     = 503
	TokenRefreshRequired         = 400
)

// Misc system default configs
const (
	// default read 128KB
	ReadBufferSize = 131072
)