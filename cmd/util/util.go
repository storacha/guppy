package util

import (
	_ "embed"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/car"
	"github.com/storacha/go-ucanto/transport/http"
	cdg "github.com/storacha/guppy/pkg/delegation"
)

const defaultServiceName = "staging.up.storacha.network"

//go:embed config.ipldsch
var configsch []byte

type configurationModel struct {
	Signer []byte
}

func MustGetSigner() principal.Signer {
	str := os.Getenv("W3UP_PRIVATE_KEY") // use env var preferably
	if str != "" {
		s, err := signer.Parse(str)
		if err != nil {
			log.Fatal(err)
		}
		return s
	}

	conf := mustReadConfig()
	s, err := signer.Decode(conf.Signer)
	if err != nil {
		log.Fatalf("decoding signer: %s", err)
	}
	return s
}

func mustLoadConfigSchema() *schema.TypeSystem {
	ts, err := ipld.LoadSchemaBytes(configsch)
	if err != nil {
		log.Fatalf("failed to load IPLD schema: %s", err)
	}
	return ts
}

func mustReadConfig() *configurationModel {
	homedir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("obtaining user home directory: %s", err)
	}

	typ := mustLoadConfigSchema().TypeByName("Configuration")
	confdir := path.Join(homedir, ".w3up")
	confpath := path.Join(confdir, "config")
	conf := configurationModel{}

	bytes, err := os.ReadFile(confpath)
	if err != nil {
		s, err := signer.Generate()
		if err != nil {
			log.Fatalf("generating signer: %s", err)
		}

		conf.Signer = s.Encode()
		bytes, err = ipld.Marshal(dagcbor.Encode, &conf, typ)
		if err != nil {
			log.Fatalf("encoding config: %s", err)
		}
		if err := os.Mkdir(confdir, 0700); err != nil {
			log.Fatalf("writing config: %s", err)
		}
		if os.WriteFile(confpath, bytes, 0600); err != nil {
			log.Fatalf("writing config: %s", err)
		}
	} else {
		_, err = ipld.Unmarshal(bytes, dagcbor.Decode, &conf, typ)
		if err != nil {
			log.Fatalf("decoding config: %s", err)
		}
	}

	return &conf
}

func MustGetConnection() client.Connection {
	// service URL & DID
	serviceURLStr := os.Getenv("STORACHA_SERVICE_URL") // use env var preferably
	if serviceURLStr == "" {
		serviceURLStr = fmt.Sprintf("https://%s", defaultServiceName)
	}

	serviceURL, err := url.Parse(serviceURLStr)
	if err != nil {
		log.Fatal(err)
	}

	serviceDIDStr := os.Getenv("STORACHA_SERVICE_DID")
	if serviceDIDStr == "" {
		serviceDIDStr = fmt.Sprintf("did:web:%s", defaultServiceName)
	}

	servicePrincipal, err := did.Parse(serviceDIDStr)
	if err != nil {
		log.Fatal(err)
	}

	// HTTP transport and CAR encoding
	channel := http.NewHTTPChannel(serviceURL)
	codec := car.NewCAROutboundCodec()

	conn, err := client.NewConnection(servicePrincipal, channel, client.WithOutboundCodec(codec))
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func MustGetReceiptsURL() *url.URL {
	receiptsURLStr := os.Getenv("STORACHA_RECEIPTS_URL")
	if receiptsURLStr == "" {
		receiptsURLStr = fmt.Sprintf("https://%s/receipt", defaultServiceName)
	}

	receiptsURL, err := url.Parse(receiptsURLStr)
	if err != nil {
		log.Fatal(err)
	}

	return receiptsURL
}

func MustParseDID(str string) did.DID {
	did, err := did.Parse(str)
	if err != nil {
		log.Fatalf("parsing DID: %s", err)
	}
	return did
}

func MustGetProof(path string) delegation.Delegation {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("reading proof file: %s", err)
	}

	proof, err := cdg.ExtractProof(b)
	if err != nil {
		log.Fatalf("extracting proof: %s", err)
	}
	return proof
}
