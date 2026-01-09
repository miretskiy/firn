//go:build darwin && arm64

package polars

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -lfirn
#include "firn.h"
*/
import "C"
