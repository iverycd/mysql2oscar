package aci

/*
#cgo CPPFLAGS: '-I./include'
#cgo windows,amd64 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/win64 -L./lib/win64 -laci
#cgo windows,386 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/win632 -L./lib/win32 -laci
#cgo linux,amd64 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/linux64 -L./lib/linux64 -laci
#cgo linux,386 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/linux32 -L./lib/linux32 -laci
#cgo linux,arm64 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/arm64 -L./lib/arm64 -laci
#cgo linux,loongarch64 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/loongarch64 -L./lib/loongarch64 -laci
#cgo linux,loong64 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/loongarchn64 -L./lib/loongarchn64 -laci
#cgo linux,mips64le LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/loongson64 -L./lib/loongson64 -laci
#cgo linux,sw64 LDFLAGS: -Wl,-rpath,$ORIGIN:$ORIGIN/lib/shenwei64 -L./lib/shenwei64 -laci
#include "aci.go.h"
*/
import "C"

// noPkgConfig is a Go tag for disabling using pkg-config and using environmental settings like CGO_CFLAGS and CGO_LDFLAGS instead

import (
	"context"
	"database/sql"
	"errors"
	"io/ioutil"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

const (
	lobBufferSize      = 4000
	useOCISessionBegin = true
	sizeOfNilPointer   = unsafe.Sizeof(unsafe.Pointer(nil))
)

type (
	// DSN is ShenTong Data Source Name
	DSN struct {
		Connect              string
		Username             string
		Password             string
		prefetchRows         C.ub4
		prefetchMemory       C.ub4
		timeLocation         *time.Location
		transactionMode      C.ub4
		enableQMPlaceholders bool
		operationMode        C.ub4
	}

	// DriverStruct is ShenTong driver struct
	DriverStruct struct {
		// Logger is used to log connection ping errors, defaults to discard
		// To log set it to something like: log.New(os.Stderr, "oci8 ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)
		Logger *log.Logger
	}

	// Connector is the sql driver connector
	Connector struct {
		// Logger is used to log connection ping errors
		Logger *log.Logger
	}

	// Conn is ShenTong connection
	Conn struct {
		svc                  *C.OCISvcCtx
		srv                  *C.OCIServer
		env                  *C.OCIEnv
		errHandle            *C.OCIError
		usrSession           *C.OCISession
		prefetchRows         C.ub4
		prefetchMemory       C.ub4
		transactionMode      C.ub4
		operationMode        C.ub4
		inTransaction        bool
		enableQMPlaceholders bool
		closed               bool
		timeLocation         *time.Location
		logger               *log.Logger
	}

	// Tx is ShenTong transaction
	Tx struct {
		conn *Conn
	}

	// Stmt is ShenTong statement
	Stmt struct {
		conn   *Conn
		stmt   *C.OCIStmt
		closed bool
	}

	// Rows is ShenTong rows
	Rows struct {
		stmt    *Stmt
		defines []defineStruct
		closed  bool
		ctx     context.Context
	}

	// Result is ShenTong result
	Result struct {
		rowsAffected    int64
		rowsAffectedErr error
		rowid           string
		rowidErr        error
		stmt            *Stmt
	}

	defineStruct struct {
		name         string
		dataType     C.ub2
		dataTypeName string
		pbuf         unsafe.Pointer
		maxSize      C.sb8
		length       *C.ub4
		indicator    *C.sb2
		defineHandle *C.OCIDefine
		subDefines   []defineStruct
	}

	bindStruct struct {
		dataType   C.ub2
		pbuf       unsafe.Pointer
		maxSize    C.sb4
		length     *C.ub2
		indicator  *C.sb2
		bindHandle *C.OCIBind
		out        sql.Out
	}
)

var (
	// ErrOCIInvalidHandle is OCI_INVALID_HANDLE
	ErrOCIInvalidHandle = errors.New("OCI_INVALID_HANDLE")
	// ErrOCISuccessWithInfo is OCI_SUCCESS_WITH_INFO
	ErrOCISuccessWithInfo = errors.New("OCI_SUCCESS_WITH_INFO")
	// ErrOCIReservedForIntUse is OCI_RESERVED_FOR_INT_USE
	ErrOCIReservedForIntUse = errors.New("OCI_RESERVED_FOR_INT_USE")
	// ErrOCINoData is OCI_NO_DATA
	ErrOCINoData = errors.New("OCI_NO_DATA")
	// ErrOCINeedData is OCI_NEED_DATA
	ErrOCINeedData = errors.New("OCI_NEED_DATA")
	// ErrOCIStillExecuting is OCI_STILL_EXECUTING
	ErrOCIStillExecuting = errors.New("OCI_STILL_EXECUTING")

	// ErrNoRowid is result has no rowid
	ErrNoRowid = errors.New("result has no rowid")

	phre           = regexp.MustCompile(`\?`)
	defaultCharset = C.ub2(0)

	typeNil       = reflect.TypeOf(nil)
	typeString    = reflect.TypeOf("a")
	typeSliceByte = reflect.TypeOf([]byte{})
	typeInt64     = reflect.TypeOf(int64(1))
	typeFloat64   = reflect.TypeOf(float64(1))
	typeTime      = reflect.TypeOf(time.Time{})

	// Driver is the sql driver
	Driver = &DriverStruct{
		Logger: log.New(ioutil.Discard, "", 0),
	}

	timeLocations []*time.Location

	byteBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, lobBufferSize)
		},
	}
)

func init() {
	sql.Register("aci", Driver)

	// set defaultCharset to AL32UTF8
	var envP *C.OCIEnv
	envPP := &envP
	var result C.sword
	result = C.OCIEnvCreate(envPP, C.OCI_DEFAULT, nil, nil, nil, nil, 0, nil)
	if result != C.OCI_SUCCESS {
		panic("OCIEnvCreate error")
	}
	nlsLang := cString("AL32UTF8")
	defaultCharset = C.OCINlsCharSetNameToId(unsafe.Pointer(*envPP), (*C.OraText)(nlsLang))
	C.free(unsafe.Pointer(nlsLang))
	C.OCIHandleFree(unsafe.Pointer(*envPP), C.OCI_HTYPE_ENV)

	// build timeLocations: GMT -12 to 14
	timeLocationNames := []string{"Etc/GMT+12", "Pacific/Pago_Pago", // -12 to -11
		"Pacific/Honolulu", "Pacific/Gambier", "Pacific/Pitcairn", "America/Phoenix", "America/Costa_Rica", // -10 to -6
		"America/Panama", "America/Puerto_Rico", "America/Punta_Arenas", "America/Noronha", "Atlantic/Cape_Verde", // -5 to -1
		"GMT",                                                                         // 0
		"Africa/Lagos", "Africa/Cairo", "Europe/Moscow", "Asia/Dubai", "Asia/Karachi", // 1 to 5
		"Asia/Dhaka", "Asia/Jakarta", "Asia/Shanghai", "Asia/Tokyo", "Australia/Brisbane", // 6 to 10
		"Pacific/Noumea", "Asia/Anadyr", "Pacific/Enderbury", "Pacific/Kiritimati", // 11 to 14
	}
	var err error
	timeLocations = make([]*time.Location, len(timeLocationNames))
	for i := 0; i < len(timeLocations); i++ {
		timeLocations[i], err = time.LoadLocation(timeLocationNames[i])
		if err != nil {
			name := "GMT"
			if i < 12 {
				name += strconv.FormatInt(int64(i-12), 10)
			} else if i > 12 {
				name += "+" + strconv.FormatInt(int64(i-12), 10)
			}
			timeLocations[i] = time.FixedZone(name, 3600*(i-12))
		}
	}
}
