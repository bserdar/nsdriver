package nsdriver

// nsdriver code is copied from
// github.com/akamai-open/netstoragekit-golang. Modifications were
// necessary because the download and upload APIs work with files on
// local filesystem, not with streams

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Netstorage struct provides all the necessary fields to
// create authorization headers.
// They are on the Akamai Netstorage account page.
// Hostname format should be "-nsu.akamaihd.net" and
// Note that don't expose Key on public repository.
// "Ssl" element is decided by "NetNetstorage" function - string "s" means https and "" does http.
type Netstorage struct {
	Hostname string
	Keyname  string
	Key      string
	Ssl      string
	Client   *http.Client
}

type StatData struct {
	Dir   string      `xml:"directory,attr"`
	Files []StatEntry `xml:"file"`
}

type StatEntry struct {
	Type   string `xml:"type,attr"`
	Name   string `xml:"name,attr"`
	Mtime  uint64 `xml:"mtime,attr"`
	Size   uint64 `xml:"size,attr"`
	MD5    string `xml:"md5,attr"`
	Target string `xml:"target,attr"`
}

type DuData struct {
	Dir    string `xml:"directory,attr"`
	DUInfo struct {
		Files uint64 `xml:"files"`
		Bytes uint64 `xml:"bytes"`
	} `xml:"du-info"`
}

// NewNetstorage func creates and initiates Netstorage struct.
// ssl parameter decides https(true) and http(false) which means "s" and "".
func NewNetstorage(hostname, keyname, key string, ssl bool) *Netstorage {
	if hostname == "" || keyname == "" || key == "" {
		panic("[NetstorageError] You should input netstorage hostname, keyname and key")
	}
	s := ""
	if ssl {
		s = "s"
	}
	return &Netstorage{hostname, keyname, key, s, http.DefaultClient}
}

func readBody(response *http.Response) ([]byte, error) {
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// buildRequest prepares the http request by creating the
// authorization headers with Netstorage struct values. The returned
// request has nil body
func (ns *Netstorage) buildRequest(action, method, nsPath string) (*http.Request, error) {
	var err error

	if u, err := url.Parse(nsPath); strings.HasPrefix(nsPath, "/") && err == nil {
		nsPath = u.RequestURI()
	} else {
		return nil, fmt.Errorf("[Netstorage Error] Invalid netstorage path: %s", nsPath)
	}

	acsAction := fmt.Sprintf("version=1&action=%s", action)
	acsAuthData := fmt.Sprintf("5, 0.0.0.0, 0.0.0.0, %d, %d, %s",
		time.Now().Unix(),
		rand.Intn(100000),
		ns.Keyname)

	signString := fmt.Sprintf("%s\nx-akamai-acs-action:%s\n", nsPath, acsAction)
	mac := hmac.New(sha256.New, []byte(ns.Key))
	mac.Write([]byte(acsAuthData + signString))
	acsAuthSign := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	request, err := http.NewRequest(method,
		fmt.Sprintf("http%s://%s%s", ns.Ssl, ns.Hostname, nsPath), nil)

	if err != nil {
		return nil, err
	}

	request.Header.Add("X-Akamai-ACS-Action", acsAction)
	request.Header.Add("X-Akamai-ACS-Auth-Data", acsAuthData)
	request.Header.Add("X-Akamai-ACS-Auth-Sign", acsAuthSign)
	request.Header.Add("Accept-Encoding", "identity")
	request.Header.Add("User-Agent", "NetStorageKit-Golang")
	return request, nil
}

// submitRequest_EmptyBody submits an http request with empty body
func (ns *Netstorage) submitRequest_EmptyBody(action, method, nsPath string) (*http.Response, error) {
	request, err := ns.buildRequest(action, method, nsPath)
	if err != nil {
		response, err := ns.Client.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode/100 != 2 {
			return response, errors.New(response.Status)
		} else {
			return response, nil
		}
	} else {
		return nil, err
	}
}

// submitRequest_GetBody submits an http request with empty body, and returns the response body contents
func (ns *Netstorage) submitRequest_GetBody(action, method, nsPath string) ([]byte, error) {
	response, err := ns.submitRequest_EmptyBody(action, method, nsPath)
	if err == nil {
		return readBody(response)
	} else {
		return nil, err
	}
}

// Du returns the disk usage information for a directory
func (ns *Netstorage) Du(nsPath string) (*DuData, error) {
	body, err := ns.submitRequest_GetBody("du&format=xml", "GET", nsPath)
	if err == nil {
		var du DuData
		if err = xml.Unmarshal(body, &du); err == nil {
			return &du, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

// Stat returns the information about an object structure
func (ns *Netstorage) Stat(nsPath string) (*StatData, error) {
	body, err := ns.submitRequest_GetBody("stat&format=xml", "GET", nsPath)
	if err == nil {
		var s StatData
		if err = xml.Unmarshal(body, &s); err == nil {
			return &s, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

// Mkdir creates an empty directory
func (ns *Netstorage) Mkdir(nsPath string) error {
	_, err := ns.submitRequest_EmptyBody("mkdir", "POST", nsPath)
	return err
}

// Rmdir deletes an empty directory
func (ns *Netstorage) Rmdir(nsPath string) error {
	_, err := ns.submitRequest_EmptyBody("rmdir", "POST", nsPath)
	return err
}

// Dir returns the directory structure in XML format
func (ns *Netstorage) Dir(nsPath string) (*StatData, error) {
	body, err := ns.submitRequest_GetBody("dir&format=xml", "GET", nsPath)
	if err == nil {
		var s StatData
		if err = xml.Unmarshal(body, &s); err == nil {
			return &s, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

// Mtime changes a file’s mtime
func (ns *Netstorage) Mtime(nsPath string, mtime int64) error {
	_, err := ns.submitRequest_EmptyBody(fmt.Sprintf("mtime&format=xml&mtime=%d", mtime), "POST", nsPath)
	return err
}

// Delete deletes an object/symbolic link
func (ns *Netstorage) Delete(nsPath string) error {
	_, err := ns.submitRequest_EmptyBody("delete", "POST", nsPath)
	return err
}

// QuickDelete deletes a directory (i.e., recursively delete a directory tree)
// In order to use this func, you need to the privilege on the CP Code.
func (ns *Netstorage) QuickDelete(nsPath string) error {
	_, err := ns.submitRequest_EmptyBody("quick-delete&quick-delete=imreallyreallysure", "POST", nsPath)
	return err
}

// Rename renames a file or symbolic link.
func (ns *Netstorage) Rename(nsTarget, nsDestination string) error {
	_, err := ns.submitRequest_EmptyBody("rename&destination="+url.QueryEscape(nsDestination), "POST", nsTarget)
	return err
}

// Symlink creates a symbolic link.
func (ns *Netstorage) Symlink(nsTarget, nsDestination string) error {
	_, err := ns.submitRequest_EmptyBody("symlink&target="+url.QueryEscape(nsTarget), "POST", nsDestination)
	return err
}

// Read submits a download request. Caller should get the contents from the response body
func (ns *Netstorage) Read(path string) (*http.Response, error) {
	if strings.HasSuffix(path, "/") {
		return nil, fmt.Errorf("[NetstorageError] Nestorage download path shouldn't be a directory: %s", path)
	}
	request, err := ns.buildRequest("download", "GET", path)
	if err != nil {
		return ns.Client.Do(request)
	} else {
		return nil, err
	}
}

// Write submits an upload request, with the content given in the source reader
func (ns *Netstorage) Write(source io.ReadCloser, destination string) error {
	request, err := ns.buildRequest("upload", "PUT", destination)
	if err != nil {
		request.Body = source
		_, err := ns.Client.Do(request)
		return err
	} else {
		return err
	}
}
