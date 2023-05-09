package azblob

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/mantissahz/backupstore"
)

var (
	log = logrus.WithFields(logrus.Fields{"pkg": "azblob"})
)

type BackupStoreDriver struct {
	destURL string
	path    string
	service *Service
}

const (
	KIND = "azblob"
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
	b.service, err = NewService(u)
	if err != nil {
		return nil, err
	}

	b.path = u.Path
	if b.service.Container == "" || b.path == "" {
		return nil, fmt.Errorf("Invalid URL. Must be either azblob://container@serviceurl/path/, or azblob://container/path")
	}

	b.path = strings.TrimLeft(b.path, "/")

	if _, err := b.List(""); err != nil {
		return nil, err
	}

	b.destURL = KIND + "://" + b.service.Container
	if b.service.Container != "" {
		b.destURL += "@" + b.service.ServiceURL
	}
	b.destURL += "/" + b.path

	log.Infof("Loaded driver for %v", b.destURL)
	return b, nil
}

func (s *BackupStoreDriver) Kind() string {
	return KIND
}

func (s *BackupStoreDriver) GetURL() string {
	return s.destURL
}

func (s *BackupStoreDriver) updatePath(path string) string {
	return filepath.Join(s.path, path)
}

func (s *BackupStoreDriver) List(listPath string) ([]string, error) {
	var result []string

	path := s.updatePath(listPath) + "/"
	contents, err := s.service.ListBlobs(path, "/")
	if err != nil {
		log.WithError(err).Error("Failed to list azblob")
		return result, err
	}

	sizeC := len(*contents)
	if sizeC == 0 {
		return result, nil
	}

	result = []string{}
	for _, blob := range *contents {
		r := strings.TrimPrefix(blob, path)
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
	head, err := s.service.GetBlobProperties(path)
	if err != nil || head.ContentLength == nil {
		return -1
	}
	return *head.ContentLength
}

func (s *BackupStoreDriver) FileTime(filePath string) time.Time {
	path := s.updatePath(filePath)
	blobProp, err := s.service.GetBlobProperties(path)
	if err != nil || blobProp.ContentLength == nil {
		return time.Time{}
	}
	return blobProp.LastModified.UTC()
}

func (s *BackupStoreDriver) Remove(path string) error {
	return s.service.DeleteBlobs(s.updatePath(path))
}

func (s *BackupStoreDriver) Read(src string) (io.ReadCloser, error) {
	path := s.updatePath(src)
	rc, err := s.service.GetBlob(path)
	if err != nil {
		return nil, err
	}
	return rc, nil
}

func (s *BackupStoreDriver) Write(dst string, rs io.ReadSeeker) error {
	path := s.updatePath(dst)
	return s.service.PutBlob(path, rs)
}

func (s *BackupStoreDriver) Upload(src, dst string) error {
	file, err := os.Open(src)
	if err != nil {
		return nil
	}
	defer file.Close()
	path := s.updatePath(dst)
	return s.service.PutBlob(path, file)
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
	rc, err := s.service.GetBlob(path)
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(f, rc)
	return err
}
