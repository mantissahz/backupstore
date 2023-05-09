package test

import (
	"math/rand"
	"os"
	"strconv"

	. "gopkg.in/check.v1"

	"github.com/mantissahz/backupstore"
	_ "github.com/mantissahz/backupstore/nfs"
	"github.com/mantissahz/backupstore/util"
)

func (s *TestSuite) TestDeleteDeltaBlockBackup(c *C) {
	compressionMethods := []string{"none", "gzip", "lz4"}
	concurrentLimit := 5
	for _, compressionMethod := range compressionMethods {
		// Make identical blocks in the file
		data := make([]byte, volumeSize)
		blockSize := int64(backupstore.DEFAULT_BLOCK_SIZE)

		for i := int64(0); i < blockSize; i++ {
			data[i] = letterBytes[rand.Intn(len(letterBytes))]
		}
		for i := int64(1); i < volumeContentSize/blockSize; i++ {
			for j := int64(0); j < blockSize; j++ {
				data[i*blockSize+j] = data[j]
			}
		}

		volumeName := "DeltaBlockTestVolume-" + compressionMethod + "-" + strconv.Itoa(concurrentLimit)
		volume := RawFileVolume{
			v: backupstore.Volume{
				Name:              volumeName,
				Size:              volumeSize,
				CreatedTime:       util.Now(),
				CompressionMethod: compressionMethod,
			},
		}
		// Each snapshot will be one more block different from before
		for i := 0; i < snapshotCounts; i++ {
			snapName := s.getSnapshotName("snapshot-", i)
			volume.Snapshots = append(volume.Snapshots,
				backupstore.Snapshot{
					Name:        snapName,
					CreatedTime: util.Now(),
				},
			)

			err := os.WriteFile(snapName, data, 0600)
			c.Assert(err, IsNil)

			s.randomChange(data, int64(0)*blockSize, 10)
		}

		backups := make(map[int]string)
		for i := 0; i < snapshotCounts; i++ {
			config := &backupstore.DeltaBackupConfig{
				Volume:          &volume.v,
				Snapshot:        &volume.Snapshots[i],
				DestURL:         s.getDestURL(),
				DeltaOps:        &volume,
				ConcurrentLimit: int32(concurrentLimit),
				Labels: map[string]string{
					"SnapshotName": volume.Snapshots[i].Name,
					"RandomKey":    "RandomValue",
				},
			}
			backup := s.createAndWaitForBackup(c, config, &volume)
			backups[i] = backup
		}

		// delete all backups starting from the last to test volume backup info
		// update
		for i := snapshotCounts; i > 0; i-- {
			err := backupstore.DeleteDeltaBlockBackup(backups[i-1])
			c.Assert(err, IsNil)
		}
	}
}
