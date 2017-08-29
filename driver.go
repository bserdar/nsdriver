package nsdriver

import (
	_ "bufio"
	"bytes"
	"errors"
	_ "fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	_ "reflect"
	_ "strconv"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
)

const (
	driverName = "netstorage"
)

type TempFileWriter interface {
	storagedriver.FileWriter
}

type Driver struct {
	// ns is the Akamai netstorage interface
	ns *Netstorage
	// local is the local driver to store
	local storagedriver.StorageDriver

	// tempFileFunc should return a temp file writer using the local storage
	tempFileFunc func(driver *Driver, nm string, append bool) (TempFileWriter, error)

	// getNameFunc maps file names to storage file names, and decides
	// whether the file should be in local storage or netstorage
	getNameFunc func(ctx context.Context, nm string) (name string, local bool)

	// urlMapperFunc rewrites the URL for the given path. If this
	// function is null, url mapper of the local driver is called
	urlMapperFunc func(ctx context.Context, path string, options map[string]interface{}) (string, error)
}

// Implement the storagedriver.StorageDriver interface

func (d *Driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte. This
// simple calls driver.Reader
func (d *Driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	rc, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PutContent stores the []byte content at a location designated by
// "path". This simple calls driver.Writer
func (d *Driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	writer, err := d.Writer(ctx, subPath, false)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, bytes.NewReader(contents))
	if err != nil {
		writer.Cancel()
		return err
	}
	return writer.Commit()
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset. It calls driver.getNameFunc() to
// determine whether the local storage or akamai store is going to be
// used for this file
func (d *Driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	mappedName, local := d.getNameFunc(ctx, path)
	if local {
		return d.local.Reader(ctx, mappedName, offset)
	} else {
		response, err := d.ns.Read(mappedName)
		if err != nil {
			return nil, err
		}
		if offset > 0 {
			return &readFrom{r: response.Body, o: offset}, nil
		} else {
			return response.Body, nil
		}
	}
}

// readFrom skips o bytes of the stream, and reads the rest
type readFrom struct {
	r    io.ReadCloser
	o    int64
	seen int64
}

func (r *readFrom) Read(p []byte) (int, error) {
	for r.seen < r.o {
		skip := r.o - r.seen
		var buf []byte
		if skip > 16384 {
			buf = make([]byte, 16384)
		} else {
			buf = make([]byte, skip)
		}
		read, err := r.r.Read(buf)
		if err != nil {
			return int(r.seen), err
		}
		r.seen += int64(read)
		if read == 0 {
			return 0, nil
		}
	}
	return r.r.Read(p)
}

func (r readFrom) Close() error {
	return r.r.Close()
}

func (d *Driver) Writer(ctx context.Context, subPath string, append bool) (storagedriver.FileWriter, error) {
	mappedName, local := d.getNameFunc(ctx, subPath)
	if local {
		return d.local.Writer(ctx, mappedName, append)
	} else {
		// Writing to akamai is problematic with the FileWriter
		// semantics. We can't append, or commit. So, we first write
		// to temporary storage, and then upon commit, we copy the
		// file to akamai
		return d.tempFileFunc(d, subPath, append)
	}
}

type LocalTempFileWriter struct {
	d            *Driver
	tempFileName string
	tempFile     *os.File
	destFileName string
}

func (t LocalTempFileWriter) Write(p []byte) (int, error) {
	return t.tempFile.Write(p)
}

func (t LocalTempFileWriter) Close() error {
	return t.tempFile.Close()
}

func (t LocalTempFileWriter) Size() int64 {
	l, err := t.tempFile.Seek(0, 2)
	if err != nil {
		return 0
	}
	return l
}

func (t LocalTempFileWriter) Cancel() error {
	t.tempFile.Close()
	os.Remove(t.tempFileName)
	return nil
}

func (t LocalTempFileWriter) Commit() error {
	t.tempFile.Seek(0, 0)
	err := t.d.ns.Write(t.tempFile, t.destFileName)
	defer func() {
		t.tempFile.Close()
		os.Remove(t.tempFileName)
	}()
	if err != nil {
		return err
	}
	return nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *Driver) Stat(ctx context.Context, subPath string) (storagedriver.FileInfo, error) {
	mappedName, local := d.getNameFunc(ctx, subPath)
	if local {
		return d.local.Stat(ctx, mappedName)
	} else {
		st, err := d.ns.Stat(mappedName)
		if err != nil {
			return nil, err
		}
		var ret storagedriver.FileInfoInternal
		ret.FileInfoFields.Path = path.Join(st.Dir, st.Files[0].Name)
		ret.FileInfoFields.ModTime = time.Unix(int64(st.Files[0].Mtime), 0)
		if st.Files[0].Type == "file" {
			ret.FileInfoFields.Size = int64(st.Files[0].Size)
		} else {
			ret.FileInfoFields.IsDir = true
		}
		return &ret, nil
	}
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *Driver) List(ctx context.Context, subPath string) ([]string, error) {
	mappedName, local := d.getNameFunc(ctx, subPath)
	if local {
		return d.local.List(ctx, mappedName)
	} else {
		st, err := d.ns.Dir(mappedName)
		if err != nil {
			return nil, err
		}
		ret := make([]string, len(st.Files))
		for i, f := range st.Files {
			ret[i] = f.Name
		}
		return ret, nil
	}
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *Driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	mappedSource, sourceLocal := d.getNameFunc(ctx, sourcePath)
	mappedDest, destLocal := d.getNameFunc(ctx, destPath)

	switch {
	case sourceLocal && destLocal:
		return d.local.Move(ctx, mappedSource, mappedDest)
	case sourceLocal && !destLocal:
		return d.moveFromLocal(ctx, mappedSource, mappedDest)
	case !sourceLocal && destLocal:
		return errors.New("Cannot move remote file to local")
	default:
		return d.ns.Rename(mappedSource, mappedDest)
	}
}

func (d *Driver) moveFromLocal(ctx context.Context, source, dest string) error {
	f, err := os.Open(source)
	if err != nil {
		return err
	}
	err = d.ns.Write(f, dest)
	if err != nil {
		return err
	}
	os.Remove(dest)
	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *Driver) Delete(ctx context.Context, subPath string) error {
	mappedName, local := d.getNameFunc(ctx, subPath)
	if local {
		return d.local.Delete(ctx, mappedName)
	} else {
		return d.ns.QuickDelete(mappedName)
	}
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *Driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	if d.urlMapperFunc == nil {
		return d.local.URLFor(ctx, path, options)
	} else {
		return d.urlMapperFunc(ctx, path.options)
	}
}
