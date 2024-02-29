package zg

import (
	"context"
	"encoding/json"
	"time"

	"github.com/0xPolygonHermez/zkevm-node/dataavailability"
	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	pb "github.com/zero-gravity-labs/zerog-data-avail/api/grpc/disperser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ZgConfig struct {
	Enable      bool   `mapstructure:"Enable"`
	Address     string `mapstructure:"Address"`
	MaxBlobSize int    `mapstructure:"MaxBlobSize"`
}

type ZgDA struct {
	Client pb.DisperserClient
	Cfg    ZgConfig
}

func NewZgDA(cfg ZgConfig) (*ZgDA, error) {
	conn, err := grpc.Dial(cfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return nil, err
	}
	// defer conn.Close()
	c := pb.NewDisperserClient(conn)

	return &ZgDA{
		Client: c,
		Cfg:    cfg,
	}, nil
}

func (d *ZgDA) Init() error {
	return nil
}

func (d *ZgDA) GetBatchL2Data(batchNum uint64, hash common.Hash, requestParams []dataavailability.BlobRequestParams) ([]byte, error) {
	var blobData = make([]byte, 0)
	for _, requestParam := range requestParams {
		log.Info("Requesting data from zgDA", "param", requestParam)

		retrieveBlobReply, err := d.Client.RetrieveBlob(context.Background(), &pb.RetrieveBlobRequest{
			BatchHeaderHash: requestParam.BatchHeaderHash,
			BlobIndex:       requestParam.BlobIndex,
		})

		if err != nil {
			return nil, err
		}

		blobData = append(blobData, retrieveBlobReply.GetData()...)
	}

	return blobData, nil
}

func (d *ZgDA) GetDaBackendType() dataavailability.DABackendType {
	return dataavailability.DataAvailabilityZg
}

func (s *ZgDA) PostSequence(ctx context.Context, batchesData [][]byte, batchesNumber []uint64, state dataavailability.StateInterface) ([]byte, error) {
	message := make([][]dataavailability.BlobRequestParams, 0)

	for i, seq := range batchesData {
		totalBlobSize := len(seq)
		statusReplys := make([]*pb.BlobStatusReply, 0)

		requestParams := make([]dataavailability.BlobRequestParams, 0)
		if totalBlobSize > 0 {
			log.Infof("BatchL2Data %s, len %d", seq, totalBlobSize)

			for idx := 0; idx < totalBlobSize; idx += s.Cfg.MaxBlobSize {
				var endIdx int
				if totalBlobSize <= idx+s.Cfg.MaxBlobSize {
					endIdx = totalBlobSize
				} else {
					endIdx = idx + s.Cfg.MaxBlobSize
				}

				blob := pb.DisperseBlobRequest{
					Data: seq[idx:endIdx],
					SecurityParams: []*pb.SecurityParams{
						{
							QuorumId:           0,
							AdversaryThreshold: 0,
							QuorumThreshold:    0,
						},
					},
				}

				log.Infof("Disperse blob range %d %d", idx, endIdx)
				blobReply, err := s.Client.DisperseBlob(ctx, &blob)
				if err != nil {
					log.Warn("Disperse blob error", "err", err)
					return nil, err
				}

				requestId := blobReply.GetRequestId()
				log.Infof("Disperse request id %s", requestId)
				for {
					statusReply, err := s.Client.GetBlobStatus(ctx, &pb.BlobStatusRequest{RequestId: requestId})

					if err != nil {
						log.Warn("Get blob status error", "err", err)
						return nil, err
					}
					log.Infof("status reply %s", statusReply)

					if statusReply.GetStatus() == pb.BlobStatus_CONFIRMED {
						blobInfo := statusReply.GetInfo()
						blobVerificationProof := blobInfo.GetBlobVerificationProof()
						blobIndex := blobVerificationProof.GetBlobIndex()
						metadata := blobVerificationProof.GetBatchMetadata()
						batchHeaderHash := metadata.GetBatchHeaderHash()

						requestParams = append(requestParams, dataavailability.BlobRequestParams{
							BatchHeaderHash: batchHeaderHash,
							BlobIndex:       blobIndex,
						})

						statusReplys = append(statusReplys, statusReply)

						break
					}

					time.Sleep(3 * time.Second)
				}
			}

			blobStatusReply, err := json.Marshal(statusReplys)
			if err != nil {
				return nil, err
			}

			blobRequestParams, err := json.Marshal(requestParams)
			if err != nil {
				return nil, err
			}

			log.Infof("blob status %s", blobStatusReply)
			state.AddCommitment(ctx, batchesNumber[i], string(blobRequestParams), string(blobStatusReply), nil)
			message = append(message, requestParams)
		}
	}

	rlpEncode, err := rlp.EncodeToBytes(&message)
	if err != nil {
		return nil, err
	}

	return rlpEncode, nil
}
