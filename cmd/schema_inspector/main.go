package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/isesword/firn/polars"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <parquet_file_path>")
	}

	parquetFile := os.Args[1]
	fmt.Printf("🔍 Inspecting Parquet file: %s\n", parquetFile)
	fmt.Println("================================================================================")

	// Read just the first few rows to inspect schema
	fmt.Println("📊 Reading sample data to inspect schema...")
	start := time.Now()

	df := polars.ReadParquetWithOptions(parquetFile, polars.ParquetOptions{
		NRows:    10, // Just read first 10 rows for schema inspection
		Parallel: true,
	})

	result, err := df.Collect()
	if err != nil {
		log.Fatalf("Error reading Parquet file: %v", err)
	}
	defer result.Release()

	elapsed := time.Since(start)
	fmt.Printf("⏱️  Schema read completed in: %v\n\n", elapsed)

	// Display basic info
	height, err := result.Height()
	if err != nil {
		log.Fatalf("Error getting height: %v", err)
	}

	fmt.Printf("📏 Sample dimensions: %d rows\n", height)
	fmt.Println()

	// Display the sample data
	fmt.Println("📋 Sample data (first 10 rows):")
	fmt.Println(result.String())
	fmt.Println()

	// Now get the full row count (this might take a while for 11GB file)
	fmt.Println("🔢 Counting total rows in dataset...")
	start = time.Now()

	countDf := polars.ReadParquet(parquetFile)
	countResult, err := countDf.Count().Collect()
	if err != nil {
		log.Fatalf("Error counting rows: %v", err)
	}
	defer countResult.Release()

	elapsed = time.Since(start)
	fmt.Printf("⏱️  Row count completed in: %v\n", elapsed)
	fmt.Println("📊 Total row count:")
	fmt.Println(countResult.String())
	fmt.Println()

	// Get column names and basic statistics
	fmt.Println("📈 Analyzing column characteristics...")
	start = time.Now()

	// Try to get some basic stats on a sample
	statsDf := polars.ReadParquetWithOptions(parquetFile, polars.ParquetOptions{
		NRows:    100000, // Sample 100k rows for stats
		Parallel: true,
	})

	// Try to describe the data (this will show data types and basic stats)
	statsResult, err := statsDf.Collect()
	if err != nil {
		log.Fatalf("Error reading sample for stats: %v", err)
	}
	defer statsResult.Release()

	elapsed = time.Since(start)
	fmt.Printf("⏱️  Statistics analysis completed in: %v\n", elapsed)

	statsHeight, _ := statsResult.Height()
	fmt.Printf("📊 Statistics sample: %d rows\n", statsHeight)
	fmt.Println()

	fmt.Println("✅ Schema inspection completed!")
	fmt.Println("💡 Use this information to design appropriate benchmark queries.")
}
