# Firn: Fast Go Bindings for Polars

## What's Implemented ✅

### Core DataFrame Operations
- **CSV I/O**: `ReadCSV()`, `ReadCSVWithOptions()` with glob patterns
- **Basic operations**: `Select()`, `Filter()`, `WithColumns()`, `Count()`, `Height()`, `Limit()`
- **Aggregations**: `Sum()`, `Mean()`, `Min()`, `Max()`, `Median()`, `First()`, `Last()`, `NUnique()`, `Std()`, `Var()`
- **GroupBy**: `GroupBy().Agg()` with multiple aggregations
- **Sorting**: `Sort()`, `SortBy()` with `Asc()`, `Desc()` modifiers
- **Multi-DataFrame**: `Concat()` for combining DataFrames
- **SQL**: `Query()` for SQL operations on DataFrames
- **Joins**: `InnerJoin()`, `LeftJoin()`, `RightJoin()`, `OuterJoin()`, `CrossJoin()` with builder pattern

### Expression System
- **Literals**: `Lit()` for int64, float64, string, bool
- **Column references**: `Col("name")`
- **Arithmetic**: `Add()`, `Sub()`, `Mul()`, `Div()`
- **Comparisons**: `Gt()`, `Lt()`, `Eq()`
- **Boolean logic**: `And()`, `Or()`, `Not()`
- **Null handling**: `IsNull()`, `IsNotNull()`
- **Aliases**: `Alias("name")`

### String Operations (Partial)
- **StrLen()** ✅ - Get string length
- **StrContains(pattern)** ✅ - Check if contains substring
- **StrToUppercase()** ✅ - Convert to uppercase
- **StrStartsWith(prefix)** ✅ - Check if starts with prefix
- **StrEndsWith(suffix)** ✅ - Check if ends with suffix
- **StrToLowercase()** ❌ - Convert to lowercase (implemented but not tested)

### Window Functions
- **Window aggregations**: `Sum().Over()`, `Mean().Over()` etc.
- **Ordered windows**: `OverOrdered(partition_by, order_by)`
- **Ranking**: `Rank()`, `DenseRank()`, `RowNumber()`
- **Offset**: `Lag(offset)`, `Lead(offset)`

### Architecture
- **RPN Stack Machine**: Single FFI call execution
- **Function pointer dispatch**: No opcode overhead
- **Move semantics**: Expressions consumed by operations
- **Memory safety**: Automatic handle cleanup
- **Performance**: 80M+ rows/second on 100M datasets

## TODO List 📋

### High Priority
1. **Fix StrToLowercase()** - Implemented but missing tests
2. **String operations missing**:
   - `StrSlice(offset, length)` - Extract substring
   - `StrReplace(pattern, replacement)` - Replace first match
   - `StrReplaceAll(pattern, replacement)` - Replace all matches
   - `StrSplit(delimiter)` - Split string into list

3. **Conditional expressions**:
   - `When().Then().Otherwise()` - Essential for data transformation
   - Nested conditional logic

4. **Math operations**:
   - `Abs()`, `Round()`, `Floor()`, `Ceil()`
   - `Pow()`, `Mod()`
   - `IsIn(values)` - membership testing

### Medium Priority
5. **I/O operations**:
   - `ReadParquet()`, `WriteParquet()` - Most requested
   - `ReadJSON()`, `WriteJSON()`

6. **Data reshaping**:
   - `Pivot()`, `Unpivot()`

7. **Additional DataFrame operations**:
   - `Unique()` - remove duplicate rows
   - `DropNulls()` - remove rows with null values
   - `Top(n)`, `Bottom(n)` - efficient top-k operations

### Low Priority
8. **Advanced features**:
   - `Cast(dtype)` - type conversion
   - `Fold()` operations
   - `Map()` operations
   - Lazy scanning: `ScanParquet()`, `ScanCSV()`

### Architecture Notes
- **Performance**: 80M+ rows/second maintained
- **Memory**: Zero-copy operations with automatic cleanup
- **Testing**: All new features need golden tests
- **Compatibility**: Match Polars semantics exactly
