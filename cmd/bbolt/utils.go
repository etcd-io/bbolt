package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"
)

func checkSourceDBPath(srcPath string) (os.FileInfo, error) {
	fi, err := os.Stat(srcPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("source database file %q doesn't exist", srcPath)
	} else if err != nil {
		return nil, fmt.Errorf("failed to open source database file %q: %v", srcPath, err)
	}
	return fi, nil
}

const FORMAT_MODES = "auto|ascii-encoded|hex|bytes|redacted"

// formatBytes converts bytes into string according to format.
// Supported formats: ascii-encoded, hex, bytes.
func formatBytes(b []byte, format string) (string, error) {
	switch format {
	case "ascii-encoded":
		return fmt.Sprintf("%q", b), nil
	case "hex":
		return fmt.Sprintf("%x", b), nil
	case "bytes":
		return string(b), nil
	case "auto":
		return bytesToAsciiOrHex(b), nil
	case "redacted":
		hash := sha256.New()
		hash.Write(b)
		return fmt.Sprintf("<redacted len:%d sha256:%x>", len(b), hash.Sum(nil)), nil
	default:
		return "", fmt.Errorf("formatBytes: unsupported format: %s", format)
	}
}

func parseBytes(str string, format string) ([]byte, error) {
	switch format {
	case "ascii-encoded":
		return []byte(str), nil
	case "hex":
		return hex.DecodeString(str)
	default:
		return nil, fmt.Errorf("parseBytes: unsupported format: %s", format)
	}
}

// writelnBytes writes the byte to the writer. Supported formats: ascii-encoded, hex, bytes, auto, redacted.
// Terminates the write with a new line symbol;
func writelnBytes(w io.Writer, b []byte, format string) error {
	str, err := formatBytes(b, format)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, str)
	return err
}

// isPrintable returns true if the string is valid unicode and contains only printable runes.
func isPrintable(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, ch := range s {
		if !unicode.IsPrint(ch) {
			return false
		}
	}
	return true
}

func bytesToAsciiOrHex(b []byte) string {
	sb := string(b)
	if isPrintable(sb) {
		return sb
	} else {
		return hex.EncodeToString(b)
	}
}

func stringToPage(str string) (uint64, error) {
	return strconv.ParseUint(str, 10, 64)
}

// stringToPages parses a slice of strings into page ids.
func stringToPages(strs []string) ([]uint64, error) {
	var a []uint64
	for _, str := range strs {
		if len(str) == 0 {
			continue
		}
		i, err := stringToPage(str)
		if err != nil {
			return nil, err
		}
		a = append(a, i)
	}
	return a, nil
}

type cmdKvStringer struct{}

func (cmdKvStringer) KeyToString(key []byte) string {
	return bytesToAsciiOrHex(key)
}

func (cmdKvStringer) ValueToString(value []byte) string {
	return bytesToAsciiOrHex(value)
}

func CmdKvStringer() bolt.KVStringer {
	return cmdKvStringer{}
}

func findLastBucket(tx *bolt.Tx, bucketNames []string) (*bolt.Bucket, error) {
	lastbucket := tx.Bucket([]byte(bucketNames[0]))
	if lastbucket == nil {
		return nil, berrors.ErrBucketNotFound
	}
	for _, bucket := range bucketNames[1:] {
		lastbucket = lastbucket.Bucket([]byte(bucket))
		if lastbucket == nil {
			return nil, berrors.ErrBucketNotFound
		}
	}
	return lastbucket, nil
}
