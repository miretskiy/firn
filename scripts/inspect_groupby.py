import polars as pl
import inspect

# Create a DataFrame and get a GroupBy object
df = pl.DataFrame({
    "department": ["Engineering", "Sales"],
    "salary": [70000, 55000],
})

grouped = df.group_by("department")

print("=== GroupBy Class Information ===")
print(f"GroupBy class: {type(grouped)}")
print(f"Module: {type(grouped).__module__}")
print()

print("=== Available Methods ===")
methods = [method for method in dir(grouped) if not method.startswith('_')]
for method in sorted(methods):
    print(f"- {method}")
print()

print("=== Method Signatures ===")
for method_name in ['len', 'sum', 'mean', 'count']:
    if hasattr(grouped, method_name):
        method = getattr(grouped, method_name)
        print(f"{method_name}:")
        print(f"  Signature: {inspect.signature(method)}")
        print(f"  Doc: {method.__doc__}")
        print()

print("=== Source Code Inspection ===")
try:
    # Try to get source code for len method
    if hasattr(grouped, 'len'):
        print("len() source code:")
        print(inspect.getsource(grouped.len))
        print()
except Exception as e:
    print(f"Could not get len() source: {e}")

try:
    # Try to get source code for sum method
    if hasattr(grouped, 'sum'):
        print("sum() source code:")
        print(inspect.getsource(grouped.sum))
        print()
except Exception as e:
    print(f"Could not get sum() source: {e}")

try:
    # Try to get source code for mean method
    if hasattr(grouped, 'mean'):
        print("mean() source code:")
        print(inspect.getsource(grouped.mean))
        print()
except Exception as e:
    print(f"Could not get mean() source: {e}")
