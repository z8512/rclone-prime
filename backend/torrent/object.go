package torrent

import (
	"context"
	"io"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
)

const (
	defaultReadAhead   int64 = 4 << 20
	largeFileReadAhead int64 = 16 << 20

	criticalWindow = 3
	prefetchWindow = 10

	priorityNow    = 255
	priorityHigh   = 192
	priorityNormal = 128
	priorityLow    = 64
)

// Object represents a file within a torrent.
type Object struct {
	fs          *Fs
	virtualPath string
	torrentPath string
	size        int64
	modTime     time.Time
	sourcePath  string
}

func (o *Object) Fs() fs.Info                           { return o.fs }
func (o *Object) Remote() string                        { return o.virtualPath }
func (o *Object) ModTime(ctx context.Context) time.Time  { return o.modTime }
func (o *Object) Size() int64                           { return o.size }
func (o *Object) Storable() bool                        { return false }
func (o *Object) String() string                        { return o.virtualPath }
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorPermissionDenied
}
func (o *Object) Remove(ctx context.Context) error {
	return fs.ErrorPermissionDenied
}
func (o *Object) Update(ctx context.Context, in io.Reader, info fs.ObjectInfo, options ...fs.OpenOption) error {
	return fs.ErrorPermissionDenied
}
