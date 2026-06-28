package isaacnetwork_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/imfact-labs/mitum2/base"
	isaacnetwork "github.com/imfact-labs/mitum2/isaac/network"
	"github.com/imfact-labs/mitum2/launch"
	"github.com/imfact-labs/mitum2/util"
	jsonenc "github.com/imfact-labs/mitum2/util/encoder/json"
	"github.com/stretchr/testify/require"
)

func TestQuicstreamHandlerGetNodeInfoFuncNetworkParams(t *testing.T) {
	networkID := base.NetworkID(util.UUID().Bytes())
	nodeinfo := isaacnetwork.NewNodeInfoUpdater(
		networkID,
		base.RandomNode(),
		util.MustNewVersion("v1.2.3"),
	)
	networkParams := &launch.NetworkParams{BaseParams: util.NewBaseParams()}
	require.NoError(t, networkParams.SetTimeoutRequest(time.Second*3))

	getNodeInfo := launch.QuicstreamHandlerGetNodeInfoFunc(jsonenc.NewEncoder(), nodeinfo, networkParams)

	timeoutRequest := func(b []byte) string {
		var u struct {
			Local struct {
				NetworkParams struct {
					TimeoutRequest string `json:"timeout_request"`
				} `json:"network_parameters"`
			} `json:"local"`
		}

		require.NoError(t, json.Unmarshal(b, &u))

		return u.Local.NetworkParams.TimeoutRequest
	}

	first, err := getNodeInfo()
	require.NoError(t, err)
	require.Equal(t, "3s", timeoutRequest(first))

	nodeInfoID := nodeinfo.ID()
	require.NoError(t, networkParams.SetTimeoutRequest(time.Second*11))
	require.Equal(t, nodeInfoID, nodeinfo.ID())

	second, err := getNodeInfo()
	require.NoError(t, err)
	require.Equal(t, "11s", timeoutRequest(second))
	require.NotEqual(t, string(first), string(second))
}
