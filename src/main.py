import pytest
import json
import csv
import threading
import time
import psutil
import os
from pathlib import Path
from BenchmarkTargets.SequentialInsert import SequentialInsert
from Database.PostgresRepository import PostgresDB

# Benchmarking setup
process = psutil.Process(os.getpid())
global_data = [i for i in range(10**6)]
OUTPUT_DIR = Path.cwd() / "output_data"
database = PostgresDB()
# Ensure parent directory exists
OUTPUT_DIR.mkdir(exist_ok=True)


def memory_sampler(interval=0.01):
    samples = []
    running = True

    def sample_loop():
        while running:
            samples.append(process.memory_info().rss / 1024)  # in KB
            time.sleep(interval)

    thread = threading.Thread(target=sample_loop, daemon=True)
    thread.start()

    def stop():
        nonlocal running
        running = False
        thread.join()

    return samples, stop


# Your benchmark tests


def test_sequential_insert(benchmark):
    def target():
        task = SequentialInsert(database=database)
        task.perform_task()

    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


def test_sum(benchmark):
    def target():
        return sum(global_data)

    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


def test_list_comprehension(benchmark):
    def target():
        return [x * 2 for x in global_data]

    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


# Programmatically run the tests
def run_tests():
    # Run tests with pytest.main() and save the result to a JSON file
    result = pytest.main(
        [
            "--benchmark-enable",
            "--benchmark-json={outputfile}".format(
                outputfile=OUTPUT_DIR / "results.json"
            ),
            Path.cwd() / "main.py",
        ]
    )

    if result != 0:
        print("There was an error running the tests.")
        return

    # Process JSON to CSV
    with open(OUTPUT_DIR / "results.json") as f:
        data = json.load(f)

    benchmarks = data["benchmarks"]

    with open(
        "{outputfile}".format(outputfile=OUTPUT_DIR / "benchmark_results.csv"),
        "w",
        newline="",
    ) as csvfile:
        fieldnames = ["name", "mean", "stddev", "ops", "avg_rss_kb", "peak_rss_kb"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for b in benchmarks:
            info = b.get("extra_info", {})
            writer.writerow(
                {
                    "name": b["fullname"],
                    "mean": b["stats"]["mean"],
                    "stddev": b["stats"]["stddev"],
                    "ops": b["stats"]["ops"],
                    "avg_rss_kb": info.get("avg_rss_kb", ""),
                    "peak_rss_kb": info.get("peak_rss_kb", ""),
                }
            )

    print("Benchmark results saved to benchmark_results.csv")


# Call the function to run everything
if __name__ == "__main__":
    run_tests()
