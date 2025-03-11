package torrent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/types"
	"github.com/dustin/go-humanize"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"
)

// ---------------------------------------------------------------------------
// Filesystem Registration & Options
// ---------------------------------------------------------------------------

var fsInfo = &fs.RegInfo{
	Name:        "torrent",
	Description: "Read-only torrent backend for accessing torrent contents",
	NewFs:       NewFs,
	Options: []fs.Option{{
		Name:     "root_directory",
		Help:     "Local directory containing torrent files.",
		Required: true,
	}, {
		Name:    "max_download_speed",
		Help:    "Maximum download speed (kBytes/s).",
		Default: 0,
	}, {
		Name:    "max_upload_speed",
		Help:    "Maximum upload speed (kBytes/s).",
		Default: 0,
	}, {
		Name:     "cache_dir",
		Help:     "Directory to store downloaded torrent data.",
		Default:  "",
		Advanced: true,
	}, {
		Name:     "cleanup_timeout",
		Help:     "Remove inactive torrents after X minutes (0 to disable).",
		Default:  0,
		Advanced: true,
	}},
}

func init() {
	fs.Register(fsInfo)
}

// Options for the torrent backend.
type Options struct {
	RootDirectory    string `config:"root_directory"`
	MaxDownloadSpeed int    `config:"max_download_speed"`
	MaxUploadSpeed   int    `config:"max_upload_speed"`
	CacheDir         string `config:"cache_dir"`
	CleanupTimeout   int    `config:"cleanup_timeout"`
}

// ---------------------------------------------------------------------------
// FS & Torrent Management Types
// ---------------------------------------------------------------------------

// Fs implements a read-only torrent filesystem and the backend commands.
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	client   *torrent.Client
	baseFs   fs.Fs

	// activeTorrents maps torrent file paths to torrentInfo.
	activeTorrents sync.Map // map[string]*torrentInfo
}

// torrentInfo tracks metadata for an active torrent.
type torrentInfo struct {
	torrent    *torrent.Torrent
	addedAt    time.Time
	lastAccess time.Time
	paused     bool
	mu         sync.RWMutex // protects lastAccess and paused
}

// ---------------------------------------------------------------------------
// Torrent Reading
// ---------------------------------------------------------------------------

type torrentReader struct {
	ctx         context.Context
	cancel      context.CancelFunc
	// Reference to the Object is still valid from object.go.
	object      *Object
	file        *torrent.File
	reader      torrent.Reader
	startTime   time.Time
	closed      atomic.Bool
	mu          sync.Mutex
	offset      int64

	readAhead   int64
	pieceLength int64
	windows     []pieceWindow
	windowsMu   sync.Mutex
}

type pieceWindow struct {
	start     int64
	end       int64
	priority  int
	readCount int64
	lastRead  time.Time
}

func (r *torrentReader) updatePiecePriorities(currentPiece int64) {
	r.windowsMu.Lock()
	defer r.windowsMu.Unlock()

	numPieces := r.file.Torrent().NumPieces()
	critical := currentPiece
	criticalEnd := min(critical+int64(criticalWindow), int64(numPieces))
	prefetch := criticalEnd
	prefetchEnd := min(prefetch+int64(prefetchWindow), int64(numPieces))

	for i := int64(0); i < int64(numPieces); i++ {
		piece := r.file.Torrent().Piece(int(i))
		if !piece.State().Complete {
			piece.SetPriority(types.PiecePriorityNone)
		}
	}

	for i := critical; i < criticalEnd; i++ {
		piece := r.file.Torrent().Piece(int(i))
		if !piece.State().Complete {
			piece.SetPriority(types.PiecePriorityNow)
			fs.Debugf(r.object, "Set critical priority for piece %d", i)
		}
	}

	for i := prefetch; i < prefetchEnd; i++ {
		piece := r.file.Torrent().Piece(int(i))
		if !piece.State().Complete {
			piece.SetPriority(types.PiecePriorityNormal)
			fs.Debugf(r.object, "Set prefetch priority for piece %d", i)
		}
	}

	r.windows = []pieceWindow{
		{start: critical, end: criticalEnd, priority: int(types.PiecePriorityNow), lastRead: time.Now()},
		{start: prefetch, end: prefetchEnd, priority: int(types.PiecePriorityNormal), lastRead: time.Now()},
	}
}

func (r *torrentReader) logProgress() {
	stats := r.file.Torrent().Stats()
	bytesCompleted := r.file.BytesCompleted()
	progress := float64(bytesCompleted) / float64(r.file.Length()) * 100

	fs.Debugf(r.object, "Progress: %.1f%%, Active peers: %d, Total peers: %d, Offset: %d/%d",
		progress, stats.ActivePeers, stats.TotalPeers, atomic.LoadInt64(&r.offset), r.file.Length())

	if r.windows != nil {
		r.windowsMu.Lock()
		for _, window := range r.windows {
			completed := 0
			for i := window.start; i < window.end; i++ {
				if r.file.Torrent().Piece(int(i)).State().Complete {
					completed++
				}
			}
			fs.Debugf(r.object, "Window [%d-%d]: %d/%d pieces complete, priority %d",
				window.start, window.end, completed, window.end-window.start, window.priority)
		}
		r.windowsMu.Unlock()
	}
}

func (r *torrentReader) Read(p []byte) (n int, err error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	r.mu.Lock()
	reader := r.reader
	r.mu.Unlock()

	if reader == nil {
		return 0, io.ErrClosedPipe
	}

	currentPos := atomic.LoadInt64(&r.offset)
	currentPiece := currentPos / r.pieceLength

	needsUpdate := false
	r.windowsMu.Lock()
	for i := range r.windows {
		if currentPiece >= r.windows[i].end {
			needsUpdate = true
			break
		}
	}
	r.windowsMu.Unlock()

	if needsUpdate {
		r.updatePiecePriorities(currentPiece)
	}

	readCtx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
	defer cancel()

	readDone := make(chan struct {
		n   int
		err error
	}, 1)

	go func() {
		n, err := reader.Read(p)
		readDone <- struct {
			n   int
			err error
		}{n, err}
	}()

	select {
	case result := <-readDone:
		if result.err == nil {
			atomic.AddInt64(&r.offset, int64(result.n))
			if time.Now().Unix()%5 == 0 {
				r.logProgress()
			}
		}
		return result.n, result.err
	case <-readCtx.Done():
		return 0, readCtx.Err()
	}
}

func (r *torrentReader) Seek(offset int64, whence int) (int64, error) {
	if r.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.reader == nil {
		return 0, io.ErrClosedPipe
	}

	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = atomic.LoadInt64(&r.offset) + offset
	case io.SeekEnd:
		abs = r.file.Length() + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if abs < 0 {
		return 0, fmt.Errorf("negative seek position: %d", abs)
	}
	if abs > r.file.Length() {
		return 0, fmt.Errorf("seek beyond end: %d > %d", abs, r.file.Length())
	}

	newPiece := abs / r.pieceLength
	r.updatePiecePriorities(newPiece)

	pos, err := r.reader.Seek(abs, io.SeekStart)
	if err == nil {
		atomic.StoreInt64(&r.offset, pos)
		fs.Debugf(r.object, "Seeked to offset %d (piece %d)", pos, newPiece)
	}

	return pos, err
}

func (r *torrentReader) RangeSeek(ctx context.Context, offset int64, whence int, length int64) (int64, error) {
	newReader := r.file.NewReader()
	pos, err := newReader.Seek(offset, whence)
	if err != nil {
		newReader.Close()
		return 0, err
	}
	r.mu.Lock()
	if oldReader := r.reader; oldReader != nil {
		oldReader.Close()
	}
	r.reader = newReader
	atomic.StoreInt64(&r.offset, pos)
	r.mu.Unlock()

	newPiece := pos / r.pieceLength
	r.updatePiecePriorities(newPiece)
	return pos, nil
}

func (r *torrentReader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	r.cancel()
	r.mu.Lock()
	reader := r.reader
	r.reader = nil
	r.mu.Unlock()
	fs.Debugf(r.object, "Closed reader after %v", time.Since(r.startTime))
	if reader != nil {
		return reader.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Object Open Method (from torrent.go)
// ---------------------------------------------------------------------------

// Open implements the fs.Object interface for torrent files.
// Note: The Object type is defined in object.go.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.Debugf(o, "Opening file: %q", o.virtualPath)

	t, err := o.fs.getTorrent(o.sourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to load torrent: %w", err)
	}

	var targetFile *torrent.File
	for _, file := range t.Files() {
		if o.torrentPath == file.DisplayPath() {
			targetFile = file
			break
		}
	}

	if targetFile == nil {
		return nil, fmt.Errorf("file not found in torrent: %s", o.virtualPath)
	}

	ctx, cancel := context.WithCancel(ctx)
	tReader := targetFile.NewReader()
	readAhead := defaultReadAhead

	if targetFile.Length() > 1<<30 { // > 1GB
		readAhead = largeFileReadAhead
	}
	tReader.SetReadahead(readAhead)

	tr := &torrentReader{
		ctx:         ctx,
		cancel:      cancel,
		object:      o,
		file:        targetFile,
		reader:      tReader,
		startTime:   time.Now(),
		readAhead:   readAhead,
		pieceLength: int64(targetFile.Torrent().Info().PieceLength),
	}

	// Initialize first piece window.
	tr.updatePiecePriorities(0)

	// Handle initial seek if provided.
	for _, option := range options {
		switch opt := option.(type) {
		case *fs.SeekOption:
			_, err = tr.Seek(opt.Offset, io.SeekStart)
			if err != nil {
				tr.Close()
				return nil, fmt.Errorf("initial seek failed: %w", err)
			}
		}
	}

	fs.Debugf(o, "Opened with read-ahead size: %d bytes", readAhead)
	return tr, nil
}

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Interface checks.
var (
	_ io.Reader      = (*torrentReader)(nil)
	_ io.Closer      = (*torrentReader)(nil)
	_ io.Seeker      = (*torrentReader)(nil)
	_ fs.RangeSeeker = (*torrentReader)(nil)
)

// ---------------------------------------------------------------------------
// Backend Command Methods on Fs
// ---------------------------------------------------------------------------

// Stats gathers torrent statistics. The filterStatus parameter is ignored in this simple implementation.
func (f *Fs) Stats(ctx context.Context, filterHash, filterStatus string) error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	f.activeTorrents.Range(func(key, value interface{}) bool {
		ti := value.(*torrentInfo)
		t := ti.torrent
		hashStr := t.InfoHash().String()
		if filterHash != "" && filterHash != hashStr {
			return true
		}
		total := t.Length()
		completed := t.BytesCompleted()
		progress := 0.0
		if total > 0 {
			progress = float64(completed) / float64(total) * 100
		}
		statusStr := "downloading"
		ti.mu.RLock()
		if ti.paused {
			statusStr = "paused"
		} else if progress >= 100 {
			statusStr = "complete"
		}
		ti.mu.RUnlock()
		stats := t.Stats()
		peers := stats.ActivePeers

		fmt.Fprintf(tw, "Hash:\t%s\n", hashStr)
		fmt.Fprintf(tw, "Name:\t%s\n", t.Name())
		fmt.Fprintf(tw, "Size:\t%s\n", humanize.Bytes(uint64(total)))
		fmt.Fprintf(tw, "Progress:\t%.1f%%\n", progress)
		fmt.Fprintf(tw, "Status:\t%s\n", statusStr)
		fmt.Fprintf(tw, "Downloaded:\t%s\n", humanize.Bytes(uint64(completed)))
		fmt.Fprintf(tw, "Uploaded:\t%s\n", humanize.Bytes(0))
		fmt.Fprintf(tw, "Download Speed:\t%s/s\n", humanize.Bytes(0))
		fmt.Fprintf(tw, "Upload Speed:\t%s/s\n", humanize.Bytes(0))
		fmt.Fprintf(tw, "Peers:\t%d\n", peers)
		fmt.Fprintf(tw, "Seeds:\t%d\n", 0)
		fmt.Fprintf(tw, "Added:\t%s\n", ti.addedAt.Format("2006-01-02 15:04:05"))
		if progress >= 100 {
			fmt.Fprintf(tw, "Completed:\t%s\n", time.Now().Format("2006-01-02 15:04:05"))
		}
		fmt.Fprintln(tw)
		return true
	})
	tw.Flush()
	return nil
}

// Pause pauses torrent downloads.
func (f *Fs) Pause(ctx context.Context, filterHash string) error {
	var found bool
	f.activeTorrents.Range(func(key, value interface{}) bool {
		ti := value.(*torrentInfo)
		t := ti.torrent
		hashStr := t.InfoHash().String()
		if filterHash != "" && filterHash != hashStr {
			return true
		}
		t.SetPaused(true)
		ti.mu.Lock()
		ti.paused = true
		ti.mu.Unlock()
		fmt.Printf("Paused torrent: %s (%s)\n", t.Name(), hashStr)
		found = true
		return true
	})
	if !found {
		return fmt.Errorf("no torrent found for hash %s", filterHash)
	}
	return nil
}

// Resume resumes paused torrents.
func (f *Fs) Resume(ctx context.Context, filterHash string) error {
	var found bool
	f.activeTorrents.Range(func(key, value interface{}) bool {
		ti := value.(*torrentInfo)
		t := ti.torrent
		hashStr := t.InfoHash().String()
		if filterHash != "" && filterHash != hashStr {
			return true
		}
		t.SetPaused(false)
		ti.mu.Lock()
		ti.paused = false
		ti.mu.Unlock()
		fmt.Printf("Resumed torrent: %s (%s)\n", t.Name(), hashStr)
		found = true
		return true
	})
	if !found {
		return fmt.Errorf("no torrent found for hash %s", filterHash)
	}
	return nil
}

// Stop stops and removes torrents.
func (f *Fs) Stop(ctx context.Context, filterHash string) error {
	var found bool
	f.activeTorrents.Range(func(key, value interface{}) bool {
		ti := value.(*torrentInfo)
		t := ti.torrent
		hashStr := t.InfoHash().String()
		if filterHash != "" && filterHash != hashStr {
			return true
		}
		t.Drop()
		f.activeTorrents.Delete(key)
		fmt.Printf("Stopped torrent: %s (%s)\n", t.Name(), hashStr)
		found = true
		return true
	})
	if !found {
		return fmt.Errorf("no torrent found for hash %s", filterHash)
	}
	return nil
}

// Verify forces a recheck of downloaded data.
func (f *Fs) Verify(ctx context.Context, filterHash string) error {
	var found bool
	f.activeTorrents.Range(func(key, value interface{}) bool {
		ti := value.(*torrentInfo)
		t := ti.torrent
		hashStr := t.InfoHash().String()
		if filterHash != "" && filterHash != hashStr {
			return true
		}
		if err := t.Verify(); err != nil {
			fmt.Printf("Verification failed for torrent: %s (%s): %v\n", t.Name(), hashStr, err)
		} else {
			fmt.Printf("Verified torrent: %s (%s)\n", t.Name(), hashStr)
		}
		found = true
		return true
	})
	if !found {
		return fmt.Errorf("no torrent found for hash %s", filterHash)
	}
	return nil
}

// Trackers displays tracker information.
func (f *Fs) Trackers(ctx context.Context, filterHash string) error {
	var found bool
	f.activeTorrents.Range(func(key, value interface{}) bool {
		ti := value.(*torrentInfo)
		t := ti.torrent
		hashStr := t.InfoHash().String()
		if filterHash != "" && filterHash != hashStr {
			return true
		}
		mi := t.Metainfo()
		if mi == nil {
			fmt.Printf("No metainfo for torrent: %s (%s)\n", t.Name(), hashStr)
			return true
		}
		fmt.Printf("Trackers for torrent: %s (%s):\n", t.Name(), hashStr)
		for i, tier := range mi.AnnounceList {
			fmt.Printf("  Tier %d:\n", i+1)
			for _, tracker := range tier {
				fmt.Printf("    %s\n", tracker)
			}
		}
		found = true
		return true
	})
	if !found {
		return fmt.Errorf("no torrent found for hash %s", filterHash)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Other Fs Methods
// ---------------------------------------------------------------------------

func (f *Fs) Name() string                   { return f.name }
func (f *Fs) Root() string                   { return f.root }
func (f *Fs) String() string                 { return fmt.Sprintf("torrent root '%s'", f.root) }
func (f *Fs) Features() *fs.Features          { return f.features }
func (f *Fs) Precision() time.Duration        { return time.Second }
func (f *Fs) Hashes() hash.Set                { return hash.Set(hash.None) }
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, fs.ErrorPermissionDenied
}
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorPermissionDenied
}
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	return nil, fs.ErrorPermissionDenied
}
func (f *Fs) Mkdir(ctx context.Context, dir string) error { return f.baseFs.Mkdir(ctx, dir) }
func (f *Fs) Rmdir(ctx context.Context, dir string) error { return f.baseFs.Rmdir(ctx, dir) }
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcRemote, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	return srcFs.baseFs.Features().DirMove(ctx, srcFs.baseFs, srcRemote, dstRemote)
}

func (f *Fs) getTorrent(path string) (*torrent.Torrent, error) {
	if v, ok := f.activeTorrents.Load(path); ok {
		ti := v.(*torrentInfo)
		ti.mu.Lock()
		ti.lastAccess = time.Now()
		ti.mu.Unlock()
		return ti.torrent, nil
	}
	fs.Debugf(nil, "Loading new torrent: %s", path)
	t, err := f.client.AddTorrentFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load torrent: %w", err)
	}
	t.AddTrackers([][]string{
		{"udp://tracker.opentrackr.org:1337/announce"},
		{"udp://tracker.openbittorrent.com:6969/announce"},
		{"udp://exodus.desync.com:6969/announce"},
		{"udp://tracker.torrent.eu.org:451/announce"},
	})
	select {
	case <-t.GotInfo():
		fs.Debugf(nil, "Got torrent metadata for: %s", t.Name())
	case <-time.After(defaultHandshakeTimeout):
		t.Drop()
		return nil, fmt.Errorf("timeout waiting for torrent metadata")
	}
	fs.Debugf(nil, "Torrent loaded: %s (pieces: %d, length: %d, trackers: %d)",
		t.Name(), t.Info().NumPieces(), t.Length(), len(t.Metainfo().AnnounceList))
	t.DownloadAll()
	ti := &torrentInfo{
		torrent:    t,
		lastAccess: time.Now(),
		addedAt:    time.Now(),
		paused:     false,
	}
	f.activeTorrents.Store(path, ti)
	if f.opt.CleanupTimeout > 0 {
		go f.cleanupTorrent(path)
	}
	go f.monitorPeerStats(t, path)
	return t, nil
}

func (f *Fs) monitorPeerStats(t *torrent.Torrent, path string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if _, exists := f.activeTorrents.Load(path); !exists {
			return
		}
		stats := t.Stats()
		fs.Debugf(nil, "Peer stats for %s - Active: %d, Total: %d, Pending: %d",
			t.Name(), stats.ActivePeers, stats.TotalPeers, stats.PendingPeers)
	}
}

func (f *Fs) cleanupTorrent(path string) {
	if f.opt.CleanupTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	timeout := time.Duration(f.opt.CleanupTimeout) * time.Minute
	for range ticker.C {
		v, ok := f.activeTorrents.Load(path)
		if !ok {
			return
		}
		ti := v.(*torrentInfo)
		ti.mu.RLock()
		inactive := time.Since(ti.lastAccess) > timeout
		ti.mu.RUnlock()
		if inactive {
			if v, ok := f.activeTorrents.LoadAndDelete(path); ok {
				ti := v.(*torrentInfo)
				ti.torrent.Drop()
				fs.Debugf(nil, "Dropped inactive torrent: %s", path)
			}
			return
		}
	}
}

func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(dir, "Listing directory")
	baseEntries, err := f.baseFs.List(ctx, dir)
	if err != nil {
		if torrentPath, isVirtual := f.findTorrentForPath(dir); isVirtual {
			fs.Debugf(dir, "Found torrent file: %s", torrentPath)
			return f.listTorrentContents(ctx, torrentPath, dir)
		}
		return nil, err
	}
	seen := make(map[string]bool)
	entries = make(fs.DirEntries, 0, len(baseEntries))
	for _, entry := range baseEntries {
		name := entry.Remote()
		if !isTorrentFile(name) {
			entries = append(entries, entry)
			seen[name] = true
		}
	}
	for _, entry := range baseEntries {
		if o, ok := entry.(fs.Object); ok && isTorrentFile(o.Remote()) {
			virtualName := strings.TrimSuffix(o.Remote(), filepath.Ext(o.Remote()))
			if !seen[virtualName] {
				if info, modTime, err := f.getTorrentInfo(o.Remote()); err == nil {
					size, items := f.getTorrentSize(info)
					entries = append(entries, &Directory{
						fs:      f,
						remote:  virtualName,
						modTime: modTime,
						size:    size,
						items:   items,
					})
					seen[virtualName] = true
				}
			}
		}
	}
	fs.Debugf(dir, "Listed %d entries", len(entries))
	return entries, nil
}

func (f *Fs) findTorrentForPath(path string) (string, bool) {
	if path == "" {
		return "", false
	}
	current := path
	for {
		torrentPath := filepath.Join(f.opt.RootDirectory, current+".torrent")
		if _, err := os.Stat(torrentPath); err == nil {
			fs.Debugf(path, "Found torrent at: %s", torrentPath)
			return torrentPath, true
		}
		parent := filepath.Dir(current)
		if parent == "." || parent == current {
			break
		}
		current = parent
	}
	return "", false
}

func (f *Fs) getTorrentInfo(path string) (*metainfo.Info, time.Time, error) {
	absPath := path
	if !filepath.IsAbs(path) {
		absPath = filepath.Join(f.opt.RootDirectory, path)
	}
	mi, err := metainfo.LoadFromFile(absPath)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to read torrent info: %w", err)
	}
	info, err := mi.UnmarshalInfo()
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to unmarshal torrent info: %w", err)
	}
	stat, err := os.Stat(absPath)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("failed to stat torrent file: %w", err)
	}
	return &info, stat.ModTime(), nil
}

type Directory struct {
	fs      *Fs
	remote  string
	modTime time.Time
	size    int64
	items   int64
}

func (d *Directory) String() string                     { return d.remote }
func (d *Directory) Remote() string                     { return d.remote }
func (d *Directory) ModTime(ctx context.Context) time.Time { return d.modTime }
func (d *Directory) Size() int64                        { return d.size }
func (d *Directory) Items() int64                       { return d.items }
func (d *Directory) ID() string                         { return "torrentdir:" + d.remote }
func (d *Directory) SetID(string)                       {}
func (d *Directory) Fs() fs.Info                        { return d.fs }

func (f *Fs) listTorrentContents(ctx context.Context, torrentPath, virtualPath string) (fs.DirEntries, error) {
	info, modTime, err := f.getTorrentInfo(torrentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read torrent info: %w", err)
	}
	torrentName := strings.TrimSuffix(filepath.Base(torrentPath), ".torrent")
	relTorrentDir, err := filepath.Rel(f.opt.RootDirectory, filepath.Dir(torrentPath))
	if err != nil {
		return nil, fmt.Errorf("failed to get relative torrent dir: %w", err)
	}
	virtualTorrentDir := filepath.Join(relTorrentDir, torrentName)
	var internalPath string
	switch {
	case virtualPath == virtualTorrentDir:
		internalPath = ""
	case strings.HasPrefix(virtualPath, virtualTorrentDir+string(filepath.Separator)):
		internalPath = strings.TrimPrefix(virtualPath, virtualTorrentDir+string(filepath.Separator))
	default:
		return nil, fmt.Errorf("path %q is not within torrent directory %q", virtualPath, virtualTorrentDir)
	}
	entries := make(fs.DirEntries, 0)
	seen := make(map[string]bool)
	if len(info.Files) == 0 {
		if internalPath == "" {
			entries = append(entries, &Object{
				fs:          f,
				virtualPath: filepath.Join(virtualPath, info.Name),
				torrentPath: info.Name,
				size:        info.Length,
				modTime:     modTime,
				sourcePath:  torrentPath,
			})
		}
		return entries, nil
	}
	prefix := ""
	if internalPath != "" {
		prefix = internalPath + string(filepath.Separator)
	}
	dirSizes := make(map[string]int64)
	dirItems := make(map[string]int64)
	for _, file := range info.Files {
		filePath := filepath.Join(file.Path...)
		if !strings.HasPrefix(filePath, prefix) {
			continue
		}
		relPath := strings.TrimPrefix(filePath, prefix)
		if relPath == "" {
			continue
		}
		components := strings.Split(relPath, string(filepath.Separator))
		firstComponent := components[0]
		if len(components) == 1 {
			entries = append(entries, &Object{
				fs:          f,
				virtualPath: filepath.Join(virtualPath, firstComponent),
				torrentPath: filePath,
				size:        file.Length,
				modTime:     modTime,
				sourcePath:  torrentPath,
			})
		} else if !seen[firstComponent] {
			currentPath := firstComponent
			for i := 0; i < len(components)-1; i++ {
				dirSizes[currentPath] += file.Length
				dirItems[currentPath]++
				if i < len(components)-2 {
					currentPath = filepath.Join(currentPath, components[i+1])
				}
			}
			entries = append(entries, &Directory{
				fs:      f,
				remote:  filepath.Join(virtualPath, firstComponent),
				modTime: modTime,
				size:    dirSizes[firstComponent],
				items:   dirItems[firstComponent],
			})
			seen[firstComponent] = true
		}
	}
	fs.Debugf(virtualPath, "Listed %d entries", len(entries))
	return entries, nil
}

func (f *Fs) getTorrentSize(info *metainfo.Info) (size, items int64) {
	if len(info.Files) == 0 {
		return info.Length, 1
	}
	for _, file := range info.Files {
		size += file.Length
		items++
	}
	return
}

func isTorrentFile(path string) bool {
	return strings.ToLower(filepath.Ext(path)) == ".torrent"
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	obj, err := f.baseFs.NewObject(ctx, remote)
	if err == nil && !isTorrentFile(remote) {
		return obj, nil
	}
	if torrentPath, isVirtual := f.findTorrentForPath(remote); isVirtual {
		info, modTime, err := f.getTorrentInfo(torrentPath)
		if err != nil {
			return nil, err
		}
		torrentRoot := strings.TrimSuffix(filepath.Base(torrentPath), ".torrent")
		relPath, err := filepath.Rel(torrentRoot, remote)
		if err != nil {
			return nil, fs.ErrorObjectNotFound
		}
		switch {
		case len(info.Files) == 0:
			if relPath == info.Name {
				return &Object{
					fs:          f,
					virtualPath: remote,
					torrentPath: info.Name,
					size:        info.Length,
					modTime:     modTime,
					sourcePath:  torrentPath,
				}, nil
			}
		default:
			for _, file := range info.Files {
				if relPath == filepath.Join(file.Path...) {
					return &Object{
						fs:          f,
						virtualPath: remote,
						torrentPath: filepath.Join(file.Path...),
						size:        file.Length,
						modTime:     modTime,
						sourcePath:  torrentPath,
					}, nil
				}
			}
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func getCacheDir(opt Options) string {
	if opt.CacheDir != "" {
		return opt.CacheDir
	}
	return filepath.Join(os.TempDir(), "rclone-torrent-cache")
}

func (f *Fs) Shutdown(ctx context.Context) error {
	f.activeTorrents.Range(func(key, value interface{}) bool {
		if ti, ok := value.(*torrentInfo); ok {
			ti.mu.Lock()
			if ti.torrent != nil {
				ti.torrent.Drop()
			}
			ti.mu.Unlock()
		}
		return true
	})
	if f.client != nil {
		f.client.Close()
	}
	if shutdowner, ok := f.baseFs.(fs.Shutdowner); ok {
		return shutdowner.Shutdown(ctx)
	}
	return nil
}

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = getCacheDir(*opt)
	if opt.MaxDownloadSpeed > 0 {
		cfg.DownloadRateLimiter = rate.NewLimiter(rate.Limit(opt.MaxDownloadSpeed*1024), 256*1024)
	}
	if opt.MaxUploadSpeed > 0 {
		cfg.UploadRateLimiter = rate.NewLimiter(rate.Limit(opt.MaxUploadSpeed*1024), 256*1024)
	}
	cfg.NoUpload = true
	cfg.Seed = false
	cfg.HandshakesTimeout = defaultHandshakeTimeout
	cfg.HalfOpenConnsPerTorrent = defaultPendingPeers
	cfg.EstablishedConnsPerTorrent = maxConnectionsPerTorrent
	cfg.DisableUTP = false
	cfg.DisableTCP = false
	cfg.NoDHT = false
	cfg.DisableIPv6 = false
	client, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create torrent client: %w", err)
	}
	baseFs, err := fs.NewFs(ctx, opt.RootDirectory)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create base filesystem: %w", err)
	}
	features := &fs.Features{
		CaseInsensitive:         false,
		DuplicateFiles:          false,
		ReadMimeType:            false,
		WriteMimeType:           false,
		CanHaveEmptyDirectories: true,
		BucketBased:             false,
		BucketBasedRootOK:       false,
		SetTier:                 false,
		GetTier:                 false,
	}
	return &Fs{
		name:     name,
		root:     root,
		opt:      *opt,
		client:   client,
		features: features,
		baseFs:   baseFs,
	}, nil
}

// ---------------------------------------------------------------------------
// Backend Command Definitions (Merged from backend.go)
// ---------------------------------------------------------------------------
