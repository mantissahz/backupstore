package s3

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "s3"})
)

type BackupStoreDriver struct {
	destURL string
	path    string
	service *service
}

const (
	KIND = "s3"
)

func init() {
	if err := backupstore.RegisterDriver(KIND, initFunc); err != nil {
		panic(err)
	}
}

func initFunc(destURL string) (backupstore.BackupStoreDriver, error) {
	u, err := url.Parse(destURL)
	if err != nil {
		return nil, err
	}

	if u.Scheme != KIND {
		return nil, fmt.Errorf("BUG: Why dispatch %v to %v?", u.Scheme, KIND)
	}

	b := &BackupStoreDriver{}
	b.service, err = newService(u)
	if err != nil {
		return nil, err
	}

	b.path = u.Path
	if b.service.Bucket == "" || b.path == "" {
		return nil, fmt.Errorf("invalid URL. Must be either s3://bucket@region/path/, or s3://bucket/path")
	}

	//Leading '/' can cause mystery problems for s3
	b.path = strings.TrimLeft(b.path, "/")

	//Test connection
	if _, err := b.List(""); err != nil {
		return nil, err
	}

	b.destURL = KIND + "://" + b.service.Bucket
	if b.service.Region != "" {
		b.destURL += "@" + b.service.Region
	}
	b.destURL += "/" + b.path

	log.Infof("Loaded driver for %v", b.destURL)
	return b, nil
}

func getCustomCerts() []byte {
	// Certificates in PEM format (base64)
	certs := os.Getenv("AWS_CERT")
	if certs == "" {
		return nil
	}

	return []byte(certs)
}

func (s *BackupStoreDriver) Kind() string {
	return KIND
}

func (s *BackupStoreDriver) GetURL() string {
	return s.destURL
}

func (s *BackupStoreDriver) updatePath(path string) string {
	joinedPath := filepath.Join(s.path, path)

	// The filepath.Join removes the trailing slash when joining paths, so we
	// need to check and add back the trailing slash if it exists in the input
	// path.
	if !strings.HasSuffix(path, "/") {
		return joinedPath
	}
	return joinedPath + "/"
}

func (s *BackupStoreDriver) List(listPath string) ([]string, error) {
	var result []string

	path := s.updatePath(listPath)
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	contents, prefixes, err := s.service.ListObjects(path, "/")
	if err != nil {
		log.WithError(err).Error("Failed to list s3")
		return result, err
	}

	sizeC := len(contents)
	sizeP := len(prefixes)
	if sizeC == 0 && sizeP == 0 {
		return result, nil
	}
	result = []string{}
	for _, obj := range contents {
		r := strings.TrimPrefix(*obj.Key, path)
		if r != "" {
			result = append(result, r)
		}
	}
	for _, p := range prefixes {
		r := strings.TrimPrefix(*p.Prefix, path)
		r = strings.TrimSuffix(r, "/")
		if r != "" {
			result = append(result, r)
		}
	}

	return result, nil
}

func (s *BackupStoreDriver) FileExists(filePath string) bool {
	return s.FileSize(filePath) >= 0
}

func (s *BackupStoreDriver) FileSize(filePath string) int64 {
	path := s.updatePath(filePath)
	head, err := s.service.HeadObject(path)
	if err != nil || head.ContentLength == nil {
		return -1
	}
	return *head.ContentLength
}

func (s *BackupStoreDriver) FileTime(filePath string) time.Time {
	path := s.updatePath(filePath)
	head, err := s.service.HeadObject(path)
	if err != nil || head.ContentLength == nil {
		return time.Time{}
	}
	return aws.TimeValue(head.LastModified).UTC()
}

func (s *BackupStoreDriver) Remove(path string) error {
	return s.service.DeleteObjects(s.updatePath(path))
}

func (s *BackupStoreDriver) Read(src string) (io.ReadCloser, error) {
	path := s.updatePath(src)
	rc, err := s.service.GetObject(path)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (s *BackupStoreDriver) Write(dst string, rs io.ReadSeeker) error {
	path := s.updatePath(dst)
	return s.service.PutObject(path, rs)
}

func (s *BackupStoreDriver) Upload(src, dst string) error {
	file, err := os.Open(src)
	if err != nil {
		return nil
	}
	defer file.Close()
	path := s.updatePath(dst)
	return s.service.PutObject(path, file)
}

func (s *BackupStoreDriver) Download(src, dst string) error {
	if _, err := os.Stat(dst); err != nil {
		os.Remove(dst)
	}

	if err := os.MkdirAll(filepath.Dir(dst), os.ModeDir|0700); err != nil {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	path := s.updatePath(src)
	rc, err := s.service.GetObject(path)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(f, rc)
	return err
}
