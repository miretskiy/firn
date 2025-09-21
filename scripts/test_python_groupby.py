import polars as pl

# Create test DataFrame
df = pl.DataFrame({
    "department": ["Engineering", "Sales", "Engineering", "Marketing", "Sales"],
    "salary": [70000, 55000, 65000, 60000, 52000],
    "age": [35, 28, 32, 30, 27],
})

print("Original DataFrame:")
print(df)
print()

# Test 1: Try count() shortcut
try:
    result = df.group_by("department").count()
    print("✅ df.group_by().count() works:")
    print(result)
    print()
except Exception as e:
    print("❌ df.group_by().count() failed:", e)
    print()

# Test 2: Try len() shortcut  
try:
    result = df.group_by("department").len()
    print("✅ df.group_by().len() works:")
    print(result)
    print()
except Exception as e:
    print("❌ df.group_by().len() failed:", e)
    print()

# Test 3: Try sum() shortcut
try:
    result = df.group_by("department").sum()
    print("✅ df.group_by().sum() works:")
    print(result)
    print()
except Exception as e:
    print("❌ df.group_by().sum() failed:", e)
    print()

# Test 4: Try mean() shortcut
try:
    result = df.group_by("department").mean()
    print("✅ df.group_by().mean() works:")
    print(result)
    print()
except Exception as e:
    print("❌ df.group_by().mean() failed:", e)
    print()

# Test 5: The agg() approach (should always work)
try:
    result = df.group_by("department").agg(pl.col("salary").count())
    print("✅ df.group_by().agg(pl.col().count()) works:")
    print(result)
    print()
except Exception as e:
    print("❌ df.group_by().agg() failed:", e)
    print()
