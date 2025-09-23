//go:build linux && amd64

package polars

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -L${SRCDIR}/../lib -lfirn_linux_amd64
#include "firn.h"
*/
import "C"
