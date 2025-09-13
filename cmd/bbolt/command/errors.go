package command

import "errors"

var (
	// ErrBatchInvalidWriteMode is returned when the write mode is other than seq, rnd, seq-nest, or rnd-nest.
	ErrBatchInvalidWriteMode = errors.New("the write mode should be one of seq, rnd, seq-nest, or rnd-nest")

	// ErrBatchNonDivisibleBatchSize is returned when the batch size can't be evenly
	// divided by the iteration count.
	ErrBatchNonDivisibleBatchSize = errors.New("the number of iterations must be divisible by the batch size")

	// ErrBucketRequired is returned when a bucket is not specified.
	ErrBucketRequired = errors.New("bucket required")

	// ErrInvalidPageArgs is returned when Page cmd receives pageIds and all option is true.
	ErrInvalidPageArgs = errors.New("invalid args: either use '--all' or 'pageid...'")

	// ErrInvalidValue is returned when a benchmark reads an unexpected value.
	ErrInvalidValue = errors.New("invalid value")

	// ErrKeyNotFound is returned when a key is not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrPageIDRequired is returned when a required page id is not specified.
	ErrPageIDRequired = errors.New("page id required")

	// ErrPathRequired is returned when the path to a bbolt database is not specified.
	ErrPathRequired = errors.New("path required")

	// ErrSurgeryFreelistAlreadyExist is returned when a bbolt database file already has a freelist.
	ErrSurgeryFreelistAlreadyExist = errors.New("the file already has freelist, please consider to abandon the freelist to forcibly rebuild it")
)
