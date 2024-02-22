package etherman

import (
	"github.com/0xPolygonHermez/zkevm-node/dataavailability"
	"github.com/ethereum/go-ethereum/common"
)

type dataAvailabilityProvider interface {
	GetBatchL2Data(batchNum uint64, hash common.Hash, requestParams []dataavailability.BlobRequestParams) ([]byte, error)

	GetDaBackendType() dataavailability.DABackendType
}
