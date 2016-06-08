package test

import (
	"flag"
	"math/rand"

	"github.com/golang/glog"
)

// DefaultFIRatio is the default fi ratio
const DefaultFIRatio = 5

var fienabled = flag.Bool("fi", false, "whether enable fault ingestion: true or false")
var firatio = flag.Int("firatio", DefaultFIRatio, "fault ingestion ratio: 0~100")

// FIEnabled returns whether fi is enabled
func FIEnabled() bool {
	return *fienabled
}

// RandomFI returns a random true or false
func RandomFI() bool {
	if *fienabled {
		ratio := *firatio
		if ratio < 0 || ratio > 100 {
			glog.Errorln("invalid firatio", ratio, "use default", DefaultFIRatio)
			ratio = DefaultFIRatio
		}

		r := rand.Int()
		b := r % (100 / ratio)
		glog.V(5).Infoln("random int", r, "ratio", ratio, b)
		return b == 0
	}
	return false
}
