//go:build linux

package robustness

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt/tests/dmflakey"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestRestartFromPowerFailure is to test data after unexpected power failure.
func TestRestartFromPowerFailure(t *testing.T) {
	flakey := initFlakeyDevice(t, t.Name(), dmflakey.FSTypeEXT4, "")
	root := flakey.RootFS()

	dbPath := filepath.Join(root, "boltdb")

	args := []string{"bbolt", "bench",
		"-work", // keep the database
		"-path", dbPath,
		"-count=1000000000",
		"-batch-size=5", // separate total count into multiple truncation
	}

	logPath := filepath.Join(t.TempDir(), fmt.Sprintf("%s.log", t.Name()))
	logFd, err := os.Create(logPath)
	require.NoError(t, err)
	defer logFd.Close()

	fpURL := "127.0.0.1:12345"

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = logFd
	cmd.Stderr = logFd
	cmd.Env = append(cmd.Env, "GOFAIL_HTTP="+fpURL)
	t.Logf("start %s", strings.Join(args, " "))
	require.NoError(t, cmd.Start(), "args: %v", args)

	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Wait()
	}()

	defer func() {
		if t.Failed() {
			logData, err := os.ReadFile(logPath)
			assert.NoError(t, err)
			t.Logf("dump log:\n: %s", string(logData))
		}
	}()

	time.Sleep(time.Duration(time.Now().UnixNano()%5+1) * time.Second)
	t.Logf("simulate power failure")

	activeFailpoint(t, fpURL, "beforeSyncMetaPage", "panic")

	select {
	case <-time.After(10 * time.Second):
		t.Error("bbolt should stop with panic in seconds")
		assert.NoError(t, cmd.Process.Kill())
	case err := <-errCh:
		require.Error(t, err)
	}
	require.NoError(t, flakey.PowerFailure(""))

	st, err := os.Stat(dbPath)
	require.NoError(t, err)
	t.Logf("db size: %d", st.Size())

	t.Logf("verify data")
	output, err := exec.Command("bbolt", "check", dbPath).CombinedOutput()
	require.NoError(t, err, "bbolt check output: %s", string(output))
}

// activeFailpoint actives the failpoint by http.
func activeFailpoint(t *testing.T, targetUrl string, fpName, fpVal string) {
	u, err := url.Parse("http://" + path.Join(targetUrl, fpName))
	require.NoError(t, err, "parse url %s", targetUrl)

	req, err := http.NewRequest("PUT", u.String(), bytes.NewBuffer([]byte(fpVal)))
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, 204, resp.StatusCode, "response body: %s", string(data))
}

// FlakeyDevice extends dmflakey.Flakey interface.
type FlakeyDevice interface {
	// RootFS returns root filesystem.
	RootFS() string

	// PowerFailure simulates power failure with drop all the writes.
	PowerFailure(mntOpt string) error

	dmflakey.Flakey
}

// initFlakeyDevice returns FlakeyDevice instance with a given filesystem.
func initFlakeyDevice(t *testing.T, name string, fsType dmflakey.FSType, mntOpt string) FlakeyDevice {
	imgDir := t.TempDir()

	flakey, err := dmflakey.InitFlakey(name, imgDir, fsType)
	require.NoError(t, err, "init flakey %s", name)
	t.Cleanup(func() {
		assert.NoError(t, flakey.Teardown())
	})

	rootDir := t.TempDir()
	err = unix.Mount(flakey.DevicePath(), rootDir, string(fsType), 0, mntOpt)
	require.NoError(t, err, "init rootfs on %s", rootDir)

	t.Cleanup(func() { assert.NoError(t, unmountAll(rootDir)) })

	return &flakeyT{
		Flakey: flakey,

		rootDir: rootDir,
		mntOpt:  mntOpt,
	}
}

type flakeyT struct {
	dmflakey.Flakey

	rootDir string
	mntOpt  string
}

// RootFS returns root filesystem.
func (f *flakeyT) RootFS() string {
	return f.rootDir
}

// PowerFailure simulates power failure with drop all the writes.
func (f *flakeyT) PowerFailure(mntOpt string) error {
	if err := f.DropWrites(); err != nil {
		return fmt.Errorf("failed to drop_writes: %w", err)
	}

	if err := unmountAll(f.rootDir); err != nil {
		return fmt.Errorf("failed to unmount rootfs %s: %w", f.rootDir, err)
	}

	if mntOpt == "" {
		mntOpt = f.mntOpt
	}

	if err := f.AllowWrites(); err != nil {
		return fmt.Errorf("failed to allow_writes: %w", err)
	}

	if err := unix.Mount(f.DevicePath(), f.rootDir, string(f.Filesystem()), 0, mntOpt); err != nil {
		return fmt.Errorf("failed to mount rootfs %s: %w", f.rootDir, err)
	}
	return nil
}

func unmountAll(target string) error {
	for i := 0; i < 50; i++ {
		if err := unix.Unmount(target, 0); err != nil {
			switch err {
			case unix.EBUSY:
				time.Sleep(500 * time.Millisecond)
				continue
			case unix.EINVAL:
				return nil
			default:
				return fmt.Errorf("failed to umount %s: %w", target, err)
			}
		}
		continue
	}
	return fmt.Errorf("failed to umount %s: %w", target, unix.EBUSY)
}
