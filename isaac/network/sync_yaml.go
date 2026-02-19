package isaacnetwork

import (
	"net/url"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	"github.com/imfact-labs/mitum2/network/quicmemberlist"
	nutil "github.com/imfact-labs/mitum2/network/util"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/encoder"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type syncSourceNodeMarshaler struct {
	Type        string `yaml:"type"`
	Address     string `yaml:"address"`
	PublicKey   string `yaml:"publickey"`
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

type syncSourceConnInfoMarshaler struct {
	Type        string `yaml:"type"`
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (p *SyncSource) MarshalYAML() (interface{}, error) {
	switch t := p.Source.(type) {
	case isaac.NodeConnInfo:
		return syncSourceNodeMarshaler{
			Type:        string(p.Type),
			Address:     t.Address().String(),
			PublicKey:   t.Publickey().String(),
			Publish:     t.ConnInfo().UDPAddr().String(),
			TLSInsecure: t.TLSInsecure(),
		}, nil
	case quicmemberlist.NamedConnInfo:
		return syncSourceConnInfoMarshaler{
			Type:        string(p.Type),
			Publish:     t.ConnInfo().UDPAddr().String(),
			TLSInsecure: t.TLSInsecure(),
		}, nil
	}
	return nil, errors.Errorf("Unknown type: %s", p.Type)
}

func (d *SyncSource) DecodeYAML(b []byte, jsonencoder encoder.Encoder) error {
	e := util.StringError("decode SyncSource")

	nb, err := util.ReplaceEnvVariables(b)
	if err != nil {
		return e.Wrap(err)
	}

	var v interface{}
	if err := yaml.Unmarshal(nb, &v); err != nil {
		return e.Wrap(err)
	}

	switch t := v.(type) {
	case string:
		if err := d.decodeString(t); err != nil {
			return e.Wrap(err)
		}

		return nil
	case map[string]interface{}:
		switch i, found := t["type"]; {
		case !found:
			return e.Errorf("missing type")
		default:
			ty, err := util.AssertInterfaceValue[string](i)
			if err != nil {
				return e.Wrap(err)
			}

			if err := d.decodeYAMLMap(ty, b, jsonencoder); err != nil {
				return e.Wrap(err)
			}

			return nil
		}

	default:
		return e.Errorf("unsupported format found, %q", string(b))
	}
}

func (d *SyncSource) decodeString(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return errors.Wrap(err, "string for SyncSource")
	}

	switch {
	case len(u.Scheme) < 1:
		return errors.Errorf("missing scheme for SyncSource")
	case len(u.Host) < 1:
		return errors.Errorf("missing host for SyncSource")
	case len(u.Port()) < 1:
		return errors.Errorf("missing port for SyncSource")
	default:
		d.Type = SyncSourceTypeURL
		d.Source = u

		return nil
	}
}

func (d *SyncSource) decodeYAMLMap(t string, b []byte, jsonencoder encoder.Encoder) error {
	ty := SyncSourceType(t)

	switch ty {
	case SyncSourceTypeNode:
		i, err := d.decodeYAMLNodeConnInfo(b, jsonencoder)
		if err != nil {
			return err
		}

		d.Type = ty
		d.Source = i

		return nil
	case SyncSourceTypeSuffrageNodes,
		SyncSourceTypeSyncSources:
		i, err := d.decodeYAMLConnInfo(b, jsonencoder)
		if err != nil {
			return err
		}

		d.Type = ty
		d.Source = i

		return nil
	default:
		return errors.Errorf("unsupported type, %q", ty)
	}
}

type syncSourceNodeUnmarshaler struct {
	Address     string `yaml:"address"`
	Publickey   string `yaml:"publickey"`
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (SyncSource) decodeYAMLNodeConnInfo(b []byte, jsonencoder encoder.Encoder) (isaac.NodeConnInfo, error) {
	e := util.StringError("decode node of SyncSource")

	var u syncSourceNodeUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return nil, e.Wrap(err)
	}

	address, err := base.DecodeAddress(u.Address, jsonencoder)
	if err != nil {
		return nil, e.Wrap(err)
	}

	pub, err := base.DecodePublickeyFromString(u.Publickey, jsonencoder)
	if err != nil {
		return nil, e.Wrap(err)
	}

	if err := nutil.IsValidAddr(u.Publish); err != nil {
		return nil, e.Wrap(err)
	}

	return NewNodeConnInfo(isaac.NewNode(pub, address), u.Publish, u.TLSInsecure)
}

type syncSourceConnInfoUnmarshaler struct {
	Publish     string `yaml:"publish"`
	TLSInsecure bool   `yaml:"tls_insecure"`
}

func (SyncSource) decodeYAMLConnInfo(
	b []byte, _ encoder.Encoder,
) (ci quicmemberlist.NamedConnInfo, _ error) {
	e := util.StringError("decode conninfo of SyncSource")

	var u syncSourceConnInfoUnmarshaler

	if err := yaml.Unmarshal(b, &u); err != nil {
		return ci, e.Wrap(err)
	}

	if err := nutil.IsValidAddr(u.Publish); err != nil {
		return ci, e.Wrap(err)
	}

	return quicmemberlist.NewNamedConnInfo(u.Publish, u.TLSInsecure)
}
