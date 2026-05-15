package measure

import (
	"fmt"
	"os"
	"time"
)

type Mode string

const (
	ModeOff    Mode = "off"
	ModePinger Mode = "pinger"
	ModePonger Mode = "ponger"
	ModeLurker Mode = "lurker"
)

type Config struct {
	Mode                   Mode
	LogDir                 string
	SampleInterval         time.Duration
	PingerInterval         time.Duration
	PeerstoreTrackInterval time.Duration
	OOBListenAddr          string
	PartnerAddr            string
	PartnerPeerID          string
}

func LoadFromEnv() (Config, error) {
	modeStr, isPresent := os.LookupEnv("IPFS_MEASURE_MODE")
	if !isPresent {
		return Config{}, fmt.Errorf("failed while reading IPFS_MEASURE_MODE from ENV")
	}
	var mode Mode
	switch modeStr {
	case "pinger":
		mode = ModePinger
	case "ponger":
		mode = ModePonger
	case "lurker":
		mode = ModeLurker
	case "off":
		mode = ModeOff
	default:
		return Config{}, fmt.Errorf("unknown IPFS_MEASURE_MODE: %q", modeStr)
	}

	logDir, _ := os.LookupEnv("IPFS_MEASURE_LOG_DIR")
	if mode != ModeOff && logDir == "" {
		return Config{}, fmt.Errorf("IPFS_MEASURE_LOG_DIR must be set when mode != off")
	}

	interval := 30 * time.Second
	if s, isPresent := os.LookupEnv("IPFS_MEASURE_SAMPLE_INTERVAL"); isPresent && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return Config{}, fmt.Errorf("invalid IPFS_MEASURE_SAMPLE_INTERVAL: %w", err)
		}
		interval = d
	}

	pingerInterval := 15 * time.Second
	if s, isPresent := os.LookupEnv("IPFS_MEASURE_PINGER_INTERVAL"); isPresent && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return Config{}, fmt.Errorf("invalid IPFS_MEASURE_PINGER_INTERVAL: %w", err)
		}
		pingerInterval = d
	}

	peerstoreTrackInterval := 30 * time.Second
	if s, isPresent := os.LookupEnv("IPFS_MEASURE_PEERSTORE_TRACK_INTERVAL"); isPresent && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return Config{}, fmt.Errorf("invalid IPFS_MEASURE_PEERSTORE_TRACK_INTERVAL: %w", err)
		}
		peerstoreTrackInterval = d
	}

	oobListenAddr, isPresent := os.LookupEnv("IPFS_MEASURE_OOB_LISTEN_ADDR")
	if mode == ModePonger && !isPresent {
		return Config{}, fmt.Errorf("IPFS_MEASURE_OOB_LISTEN_ADDR must be set for Ponger")
	}

	partnerAddr, isPresent := os.LookupEnv("IPFS_MEASURE_PARTNER_ADDR")
	if mode == ModePinger && !isPresent {
		return Config{}, fmt.Errorf("IPFS_MEASURE_PARTNER_ADDR must be set for Pinger")
	}

	partnerPeerID, isPresent := os.LookupEnv("IPFS_MEASURE_PARTNER_PEER_ID")
	if mode == ModePinger && !isPresent {
		return Config{}, fmt.Errorf("IPFS_MEASURE_PARTNER_PEER_ID must be set for Pinger")
	}

	return Config{
		Mode:                   mode,
		LogDir:                 logDir,
		SampleInterval:         interval,
		PingerInterval:         pingerInterval,
		PeerstoreTrackInterval: peerstoreTrackInterval,
		OOBListenAddr:          oobListenAddr,
		PartnerAddr:            partnerAddr,
		PartnerPeerID:          partnerPeerID,
	}, nil
}
