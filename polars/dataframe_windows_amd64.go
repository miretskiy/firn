//go:build windows && amd64

package polars

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -L${SRCDIR}/../lib -lfirn
#include "firn.h"
*/
import "C"
