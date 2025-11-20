package cmdutil_test

import (
	"testing"

	"github.com/storacha/guppy/internal/cmdutil"
	"github.com/stretchr/testify/require"
)

func TestContentPath(t *testing.T) {
	testCases := []struct {
		name            string
		pathString      string
		expectedCID     string
		expectedSubpath string
		expectedError   string
	}{
		{
			name:            "ipfs path with CID",
			pathString:      "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
			expectedCID:     "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
			expectedSubpath: "",
		},
		{
			name:            "ipfs path with CID and path",
			pathString:      "/ipfs/bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq/wiki/Vincent_van_Gogh.html",
			expectedCID:     "bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq",
			expectedSubpath: "wiki/Vincent_van_Gogh.html",
		},
		{
			name:          "ipns path",
			pathString:    "/ipns/tr.wikipedia-on-ipfs.org/wiki/",
			expectedError: "invalid path, only /ipfs/ is supported",
		},
		{
			name:            "ipfs URI with CID",
			pathString:      "ipfs://bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
			expectedCID:     "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
			expectedSubpath: "",
		},
		{
			name:            "ipfs URI with CID and path",
			pathString:      "ipfs://bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq/wiki/Vincent_van_Gogh.html",
			expectedCID:     "bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq",
			expectedSubpath: "wiki/Vincent_van_Gogh.html",
		},
		{
			name:          "other URI",
			pathString:    "example://bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq/wiki/Vincent_van_Gogh.html",
			expectedError: "invalid URI, only ipfs:// is supported",
		},
		{
			name:            "bare CID",
			pathString:      "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
			expectedCID:     "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
			expectedSubpath: "",
		},
		{
			name:            "bare CID and path",
			pathString:      "bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq/wiki/Vincent_van_Gogh.html",
			expectedCID:     "bafybeiemxf5abjwjbikoz4mc3a3dla6ual3jsgpdr4cjr3oz3evfyavhwq",
			expectedSubpath: "wiki/Vincent_van_Gogh.html",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cid, path, err := cmdutil.ContentPath(tc.pathString)
			if tc.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expectedCID, cid.String())
				require.Equal(t, tc.expectedSubpath, path)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			}
		})
	}
}
