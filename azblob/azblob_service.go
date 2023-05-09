package azblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"
)

const (
	azureURL           = "core.windows.net"
	azureServiceURL    = "%s.blob." + azureURL
	azureConnNameKey   = "AccountName=%s;AccountKey=%s;"
	blobEndpoint       = "BlobEndpoint=%s;"
	blobEndpointSuffix = "EndpointSuffix=%s"

	downloadMaxRetryRequests = 1024
)

type Service struct {
	Container       string
	ServiceURL      string
	ContainerClient azblob.ContainerClient
}

func NewService(u *url.URL) (*Service, error) {
	s := Service{}
	if u.User != nil {
		s.ServiceURL = u.Host
		s.Container = u.User.Username()
	} else {
		s.Container = u.Host
	}

	accountName := os.Getenv("AZBLOB_ACCOUNT_NAME")
	accountKey := os.Getenv("AZBLOB_ACCOUNT_KEY")
	azureEndpoint := os.Getenv("AZURE_ENDPOINT")

	// cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	// if err != nil {
	// 	return nil, err
	// }
	if s.ServiceURL == "" {
		s.ServiceURL = azureURL
	}
	var endpointSuffix string
	if !strings.Contains(s.ServiceURL, azureURL) {
		endpointSuffix = fmt.Sprintf(blobEndpointSuffix, s.ServiceURL)
	}

	// var serviceClient azblob.ServiceClient
	connStr := fmt.Sprintf(azureConnNameKey, accountName, accountKey)
	if azureEndpoint != "" {
		blobEndpointURL := strings.TrimRight(azureEndpoint, "/") + "/" + accountName
		connStr = fmt.Sprintf(connStr+blobEndpoint, blobEndpointURL)
	} else if endpointSuffix != "" {
		connStr = fmt.Sprintf(connStr+blobEndpoint, endpointSuffix)
	}

	serviceClient, err := azblob.NewServiceClientFromConnectionString(connStr, nil)
	// serviceURL := fmt.Sprintf(azureServiceURL, accountName)
	// serviceClient, err = azblob.NewServiceClientWithSharedKey(serviceURL, cred, nil)
	if err != nil {
		return nil, err
	}

	s.ContainerClient = serviceClient.NewContainerClient(s.Container)
	return &s, nil
}

func (s *Service) Close() {
}

func (s *Service) ListBlobs(prefix, delimiter string) (*[]string, error) {
	listOptions := &azblob.ContainerListBlobHierarchySegmentOptions{Prefix: &prefix}
	pager := s.ContainerClient.ListBlobsHierarchy(delimiter, listOptions)

	var blobs []string
	for pager.NextPage(context.Background()) {
		resp := pager.PageResponse()
		for _, v := range resp.ContainerListBlobHierarchySegmentResult.Segment.BlobItems {
			blobs = append(blobs, *v.Name)
		}
		for _, v := range resp.ContainerListBlobHierarchySegmentResult.Segment.BlobPrefixes {
			blobs = append(blobs, *v.Name)
		}
	}

	if err := pager.Err(); err != nil {
		return nil, err
	}

	return &blobs, nil
}

func (s *Service) GetBlobProperties(blob string) (*azblob.GetBlobPropertiesResponse, error) {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	response, err := blobClient.GetProperties(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func (s *Service) PutBlob(blob string, reader io.ReadSeeker) error {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	_, err := blobClient.Upload(context.Background(), streaming.NopCloser(reader), nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) GetBlob(blob string) (io.ReadCloser, error) {
	blobClient := s.ContainerClient.NewBlockBlobClient(blob)

	response, err := blobClient.Download(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	return response.Body(&azblob.RetryReaderOptions{MaxRetryRequests: downloadMaxRetryRequests}), nil
}

func (s *Service) DeleteBlobs(blob string) error {
	blobs, err := s.ListBlobs(blob, "")
	if err != nil {
		return errors.Wrapf(err, "failed to list blobs with prefix %v before removing them", blob)
	}

	var deletionFailures []string
	for _, blob := range *blobs {
		blobClient := s.ContainerClient.NewBlockBlobClient(blob)
		_, err = blobClient.Delete(context.Background(), nil)
		if err != nil {
			log.WithError(err).Errorf("Failed to delete blob object: %v", blob)
			deletionFailures = append(deletionFailures, blob)
		}
	}

	if len(deletionFailures) > 0 {
		return fmt.Errorf("failed to delete blobs %v", deletionFailures)
	}

	return nil
}
