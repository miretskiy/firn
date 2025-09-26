package polars

// DataType represents a Polars data type using bit-packed encoding
type DataType uint32

// Type families (high 16 bits)
const (
	FamilyInteger  = 0x0000_0000 // 0x0000_XXXX
	FamilyFloat    = 0x0001_0000 // 0x0001_XXXX  
	FamilyString   = 0x0002_0000 // 0x0002_XXXX
	FamilyTemporal = 0x0003_0000 // 0x0003_XXXX
	FamilyBoolean  = 0x0004_0000 // 0x0004_XXXX
)

// DataType constants using bit-packed encoding
const (
	// Integer types (0x0000_XXXX)
	Int8   DataType = FamilyInteger | 0x0001
	Int16  DataType = FamilyInteger | 0x0002  
	Int32  DataType = FamilyInteger | 0x0003
	Int64  DataType = FamilyInteger | 0x0004
	UInt8  DataType = FamilyInteger | 0x0005
	UInt16 DataType = FamilyInteger | 0x0006
	UInt32 DataType = FamilyInteger | 0x0007
	UInt64 DataType = FamilyInteger | 0x0008

	// Float types (0x0001_XXXX)
	Float32 DataType = FamilyFloat | 0x0001
	Float64 DataType = FamilyFloat | 0x0002
	
	// String types (0x0002_XXXX)
	String DataType = FamilyString | 0x0001
	
	// Temporal types (0x0003_XXXX) 
	Date           DataType = FamilyTemporal | 0x0001
	Time           DataType = FamilyTemporal | 0x0002
	DatetimeNanos  DataType = FamilyTemporal | 0x0003  // Nanoseconds
	DatetimeMicros DataType = FamilyTemporal | 0x0004  // Microseconds  
	DatetimeMillis DataType = FamilyTemporal | 0x0005  // Milliseconds
	DatetimeSeconds DataType = FamilyTemporal | 0x0006 // Seconds
	
	// Boolean (0x0004_XXXX)
	Boolean DataType = FamilyBoolean | 0x0001
)
