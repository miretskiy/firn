use crate::RawStr;
/// Opcodes for DataFrame and Expression operations
/// These replace function pointers for cleaner dispatch and context handling
///
/// IMPORTANT: When adding/changing opcodes here, update pkg/polars/opcodes.go
/// to match these exact values! The Go constants must stay in sync.
use std::os::raw::c_int;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpCode {
    // DataFrame operations
    NewEmpty = 1,
    ReadCsv = 2,
    ReadParquet = 3,
    Select = 4,
    SelectExpr = 5,
    Count = 6,
    Concat = 7,
    WithColumn = 8,
    FilterExpr = 9,
    GroupBy = 10,
    AddNullRow = 11,
    Collect = 12,
    Agg = 13,
    Sort = 14,
    Limit = 15,
    Query = 16,
    Join = 17,
    FromMemory = 18,

    // Expression operations (stack-based)
    ExprColumn = 100,
    ExprLiteral = 101,
    ExprAdd = 102,
    ExprSub = 103,
    ExprMul = 104,
    ExprDiv = 105,
    ExprGt = 106,
    ExprLt = 107,
    ExprEq = 108,
    ExprAnd = 109,
    ExprOr = 110,
    ExprNot = 111,
    ExprSum = 112,
    ExprMean = 113,
    ExprMin = 114,
    ExprMax = 115,
    ExprStd = 116,
    ExprVar = 117,
    ExprMedian = 118,
    ExprFirst = 119,
    ExprLast = 120,
    ExprNUnique = 121,
    ExprCount = 122,
    ExprCountNulls = 123,
    ExprIsNull = 124,
    ExprIsNotNull = 125,
    ExprAlias = 126,
    ExprStrLen = 127,
    ExprStrContains = 128,
    ExprStrStartsWith = 129,
    ExprStrEndsWith = 130,
    ExprStrToLowercase = 131,
    ExprStrToUppercase = 132,
    ExprSql = 133,
    ExprStrSlice = 134,
    ExprStrReplace = 135,
    ExprStrSplit = 136,

    // Advanced string operations (Tier 2)
    ExprStrLenBytes = 137,
    ExprStrStripChars = 138,
    ExprStrStripStart = 139,
    ExprStrStripEnd = 140,
    ExprStrStripPrefix = 170,
    ExprStrStripSuffix = 171,
    ExprStrReverse = 173,
    ExprStrHead = 174,
    ExprStrTail = 175,
    ExprStrPadStart = 176,
    ExprStrPadEnd = 177,
    ExprStrZfill = 178,

    // Window function operations
    ExprOver = 180,       // Applies window context to previous expression
    ExprRank = 181,       // Rank() function
    ExprDenseRank = 182,  // DenseRank() function
    ExprRowNumber = 183,  // RowNumber() function
    ExprLag = 184,        // Lag(n) function
    ExprLead = 185,       // Lead(n) function

    // Conditional expressions (When/Then/Otherwise)
    ExprWhen = 190,       // Start conditional chain
    ExprThen = 191,       // Pair with most recent When
    ExprOtherwise = 192,  // Finalize conditional chain

    // Cast operations
    ExprCast = 200,       // Cast expression to specified data type

    // Error operation for fluent API error handling
    Error = 999,
}

impl OpCode {
    /// Convert from u32 to OpCode
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(OpCode::NewEmpty),
            2 => Some(OpCode::ReadCsv),
            3 => Some(OpCode::ReadParquet),
            4 => Some(OpCode::Select),
            5 => Some(OpCode::SelectExpr),
            6 => Some(OpCode::Count),
            7 => Some(OpCode::Concat),
            8 => Some(OpCode::WithColumn),
            9 => Some(OpCode::FilterExpr),
            10 => Some(OpCode::GroupBy),
            11 => Some(OpCode::AddNullRow),
            12 => Some(OpCode::Collect),
            13 => Some(OpCode::Agg),
            14 => Some(OpCode::Sort),
            15 => Some(OpCode::Limit),
            16 => Some(OpCode::Query),
            17 => Some(OpCode::Join),
            18 => Some(OpCode::FromMemory),
            100 => Some(OpCode::ExprColumn),
            101 => Some(OpCode::ExprLiteral),
            102 => Some(OpCode::ExprAdd),
            103 => Some(OpCode::ExprSub),
            104 => Some(OpCode::ExprMul),
            105 => Some(OpCode::ExprDiv),
            106 => Some(OpCode::ExprGt),
            107 => Some(OpCode::ExprLt),
            108 => Some(OpCode::ExprEq),
            109 => Some(OpCode::ExprAnd),
            110 => Some(OpCode::ExprOr),
            111 => Some(OpCode::ExprNot),
            112 => Some(OpCode::ExprSum),
            113 => Some(OpCode::ExprMean),
            114 => Some(OpCode::ExprMin),
            115 => Some(OpCode::ExprMax),
            116 => Some(OpCode::ExprStd),
            117 => Some(OpCode::ExprVar),
            118 => Some(OpCode::ExprMedian),
            119 => Some(OpCode::ExprFirst),
            120 => Some(OpCode::ExprLast),
            121 => Some(OpCode::ExprNUnique),
            122 => Some(OpCode::ExprCount),
            123 => Some(OpCode::ExprCountNulls),
            124 => Some(OpCode::ExprIsNull),
            125 => Some(OpCode::ExprIsNotNull),
            126 => Some(OpCode::ExprAlias),
            127 => Some(OpCode::ExprStrLen),
            128 => Some(OpCode::ExprStrContains),
            129 => Some(OpCode::ExprStrStartsWith),
            130 => Some(OpCode::ExprStrEndsWith),
            131 => Some(OpCode::ExprStrToLowercase),
            132 => Some(OpCode::ExprStrToUppercase),
            133 => Some(OpCode::ExprSql),
            134 => Some(OpCode::ExprStrSlice),
            135 => Some(OpCode::ExprStrReplace),
            136 => Some(OpCode::ExprStrSplit),
            // Advanced string operations
            137 => Some(OpCode::ExprStrLenBytes),
            138 => Some(OpCode::ExprStrStripChars),
            139 => Some(OpCode::ExprStrStripStart),
            140 => Some(OpCode::ExprStrStripEnd),
            170 => Some(OpCode::ExprStrStripPrefix),
            171 => Some(OpCode::ExprStrStripSuffix),
            173 => Some(OpCode::ExprStrReverse),
            174 => Some(OpCode::ExprStrHead),
            175 => Some(OpCode::ExprStrTail),
            176 => Some(OpCode::ExprStrPadStart),
            177 => Some(OpCode::ExprStrPadEnd),
            178 => Some(OpCode::ExprStrZfill),
            // Window functions
            180 => Some(OpCode::ExprOver),
            181 => Some(OpCode::ExprRank),
            182 => Some(OpCode::ExprDenseRank),
            183 => Some(OpCode::ExprRowNumber),
            184 => Some(OpCode::ExprLag),
            185 => Some(OpCode::ExprLead),
            // Conditional expressions
            190 => Some(OpCode::ExprWhen),
            191 => Some(OpCode::ExprThen),
            192 => Some(OpCode::ExprOtherwise),
            // Cast
            200 => Some(OpCode::ExprCast),
            999 => Some(OpCode::Error),
            _ => None,
        }
    }

    /// Check if this is an expression operation (operates on expression stack)
    pub fn is_expression_op(&self) -> bool {
        (*self as u32) >= 100 && (*self as u32) < 999
    }

    /// Check if this is a DataFrame operation
    pub fn is_dataframe_op(&self) -> bool {
        (*self as u32) < 100
    }
}

/// Context types that Polars operations can return
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContextType {
    DataFrame = 1,   // Concrete DataFrame (can call collect, string methods)
    LazyFrame = 2,   // Lazy DataFrame (can call select, filter, with_columns, group_by)
    LazyGroupBy = 3, // Grouped lazy frame (must call agg before other operations)
}

impl ContextType {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(ContextType::DataFrame),
            2 => Some(ContextType::LazyFrame),
            3 => Some(ContextType::LazyGroupBy),
            _ => None,
        }
    }

    /// Get a human-readable name for the context type
    pub fn name(&self) -> &'static str {
        match self {
            ContextType::DataFrame => "DataFrame",
            ContextType::LazyFrame => "LazyFrame",
            ContextType::LazyGroupBy => "LazyGroupBy",
        }
    }
}

/// Sort direction for individual columns
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub enum SortDirection {
    Ascending = 0,
    Descending = 1,
}

impl SortDirection {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(SortDirection::Ascending),
            1 => Some(SortDirection::Descending),
            _ => None,
        }
    }
}

/// Nulls ordering options
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub enum NullsOrdering {
    First = 0, // Nulls appear first
    Last = 1,  // Nulls appear last
}

impl NullsOrdering {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(NullsOrdering::First),
            1 => Some(NullsOrdering::Last),
            _ => None,
        }
    }
}

/// Enhanced handle that tracks both the handle and its type
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PolarsHandle {
    pub handle: usize,
    pub context_type: u32, // ContextType as u32 for C compatibility
}

impl PolarsHandle {
    pub fn new(handle: usize, context_type: ContextType) -> Self {
        Self {
            handle,
            context_type: context_type as u32,
        }
    }

    pub fn get_context_type(&self) -> Option<ContextType> {
        ContextType::from_u32(self.context_type)
    }
}

/// Operation struct using opcodes instead of function pointers
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Operation {
    pub opcode: u32, // OpCode as u32 for C compatibility
    pub args: usize, // Arguments for the operation
}

impl Operation {
    pub fn get_opcode(&self) -> Option<OpCode> {
        OpCode::from_u32(self.opcode)
    }
}

/// A single sort field with column name, direction, and nulls ordering
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SortField {
    pub column: RawStr,
    pub direction: SortDirection,      // Default: Ascending
    pub nulls_ordering: NullsOrdering, // Default: Last
}

/// Arguments for sort operations with full directionality support
#[repr(C)]
pub struct SortArgs {
    pub fields: *const SortField,
    pub field_count: c_int,
}

/// Arguments for limit operations
#[repr(C)]
pub struct LimitArgs {
    pub n: usize,
}

/// Arguments for SQL query operations
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryArgs {
    pub sql: RawStr,
}

/// Arguments for SQL expression operations
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SqlExprArgs {
    pub sql: RawStr,
}
