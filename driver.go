// Docker registry driver for Akamai Net storage.
//
// This driver can be used as-is, or customized with filename/url
// mapper functions. When used as is, it replicates the directory
// structure of Docker registry on NetStorage. The getNameFunc and
// urlMapperFunc can be overriden, giving control to the overriding
// module a chance to customize the directory structure. These may
// also request certain types of files to be stored "locally", using
// one of the available storage drivers.
//
package nsdriver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const (
	driverName = "netstorage"
)

var (
	// overrideDriverFunc allows other modules to intercept driver
	// construction to override driver functions
	overrideDriverFunc func(*Driver)
)

// RegisterOverrideFunc registers a function that will be called after
// the driver is initialized, but before it is returned, so another
// driver may override functions of the driver
func RegisterOverrideFunc(f func(*Driver)) {
	overrideDriverFunc = f
}

// TempFileWriter writes data to a temporary file. When Commit() is
// called, the writer must copy the stored data to its NetStorage
// location, and remove the temporary data
type TempFileWriter interface {
	storagedriver.FileWriter
}

type Driver struct {
	// ns is the Akamai netstorage interface
	ns *Netstorage
	// local is the local driver to store. This defaults to filestorage driver
	Local storagedriver.StorageDriver

	// tempFileFunc should return a temp file writer using the local
	// storage. This defaults to LocalTempFileWriter
	TempFileFunc func(driver *Driver, nm string, append bool) (TempFileWriter, error)

	// getNameFunc maps file names to storage file names, and decides
	// whether the file should be in local storage or netstorage. This defaults to noop
	GetNameFunc func(ctx context.Context, d *Driver, nm string) (name string, local bool)

	// urlMapperFunc rewrites the URL for the given path. If this
	// function is null, url mapper of the local driver is called.
	UrlMapperFunc func(ctx context.Context, d *Driver, path string, options map[string]interface{}) (string, error)

	// Options used to initialize the Driver. Driver functions may look at these parameters
	Options map[string]interface{}
}

func init() {
	factory.Register(driverName, &nsDriverFactory{})
}

// nsDriverFactory implements the factory.StorageDriverFactory interface
type nsDriverFactory struct{}

// Create returns a new driver from the configuration parameters. It expects to see:
//
//    # nsdriver params:
//       hostname: string
//       keyname: string
//       key: string
//       ssl: bool
//       tmp: string (temp file directory, used by LocalTempFileFunc, defaults to OS default)
//       localDriver: (optional)
//          driverName:
//              local driver configuration
func (f *nsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	var driver Driver
	driver.Options = parameters
	if parameters != nil {
		var (
			hostname, keyname, key string
			ssl                    bool
			err                    error
		)
		if s, ok := parameters["hostname"]; ok {
			hostname = fmt.Sprint(s)
		} else {
			return nil, fmt.Errorf("hostname required")
		}
		if s, ok := parameters["keyname"]; ok {
			keyname = fmt.Sprint(s)
		} else {
			return nil, fmt.Errorf("keyname required")
		}
		if s, ok := parameters["key"]; ok {
			key = fmt.Sprint(s)
		} else {
			return nil, fmt.Errorf("key required")
		}
		if s, ok := parameters["ssl"]; ok {
			switch k := s.(type) {
			case bool:
				ssl = k
			case string:
				ssl, err = strconv.ParseBool(k)
				if err != nil {
					return nil, fmt.Errorf("invalid ssl value %s", s)
				}
			default:
				return nil, fmt.Errorf("invalid ssl value %s", s)
			}
		}
		driver.ns = NewNetstorage(hostname, keyname, key, ssl)

		if s, ok := parameters["localDriver"]; ok {
			if driverBlock, ok := s.(map[string]interface{}); ok {
				if len(driverBlock) == 1 { // There can be only one local driver
					for driverName, driverOptions := range driverBlock {
						var options map[string]interface{}
						if driverOptions == nil {
							options = nil
						} else {
							if o, ok := driverOptions.(map[string]interface{}); ok {
								options = o
							} else {
								return nil, fmt.Errorf("Invalid local driver options")
							}
						}
						driver.Local, err = factory.Create(driverName, options)
					}

				} else {
					return nil, fmt.Errorf("There can be only one local driver")
				}
			}
		}
	}
	// We made it here. Set default implementation of functions, and let other driver override them
	driver.TempFileFunc = LocalTempFileWriterFunc
	driver.GetNameFunc = func(ctx context.Context, d *Driver, nm string) (string, bool) { return nm, false }
	if overrideDriverFunc != nil {
		overrideDriverFunc(&driver)
	}
	return &driver, nil
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
	mappedName, local := d.GetNameFunc(ctx, d, path)
	if local {
		return d.Local.Reader(ctx, mappedName, offset)
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
	mappedName, local := d.GetNameFunc(ctx, d, subPath)
	if local {
		return d.Local.Writer(ctx, mappedName, append)
	} else {
		// Writing to akamai is problematic with the FileWriter
		// semantics. We can't append, or commit. So, we first write
		// to temporary storage, and then upon commit, we copy the
		// file to akamai
		return d.TempFileFunc(d, subPath, append)
	}
}

// LocalTempFileWriterFunc is the default implementation of the driver
// temp file func. It uses the "tmp" option of the driver as a
// directory to store temp files, defaults to OS default
func LocalTempFileWriterFunc(d *Driver, path string, append bool) (TempFileWriter, error) {
	var tempDir string
	// Do we have a temp file dir?
	if s, ok := d.Options["tmp"]; ok {
		tempDir = fmt.Sprint(s)
	} else {
		tempDir = os.TempDir()
	}
	tempFile, err := ioutil.TempFile(tempDir, "nsd")
	if err != nil {
		return nil, err
	}
	return LocalTempFileWriter{d: d, tempFileName: tempFile.Name(), tempFile: tempFile, destFileName: path}, nil
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
	mappedName, local := d.GetNameFunc(ctx, d, subPath)
	if local {
		return d.Local.Stat(ctx, mappedName)
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
	mappedName, local := d.GetNameFunc(ctx, d, subPath)
	if local {
		return d.Local.List(ctx, mappedName)
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
	mappedSource, sourceLocal := d.GetNameFunc(ctx, d, sourcePath)
	mappedDest, destLocal := d.GetNameFunc(ctx, d, destPath)

	switch {
	case sourceLocal && destLocal:
		return d.Local.Move(ctx, mappedSource, mappedDest)
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
	mappedName, local := d.GetNameFunc(ctx, d, subPath)
	if local {
		return d.Local.Delete(ctx, mappedName)
	} else {
		return d.ns.QuickDelete(mappedName)
	}
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *Driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	if d.UrlMapperFunc == nil {
		return d.Local.URLFor(ctx, path, options)
	} else {
		return d.UrlMapperFunc(ctx, d, path, options)
	}
}
