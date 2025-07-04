import pytest
import json
import csv
import threading
import time
import psutil
import os
import sys
from pathlib import Path
from BenchmarkTargets.SequentialInsert import SequentialInsert
from BenchmarkTargets.SequentialInsertBulk import SequentialInsertBulk
from BenchmarkTargets.BulkInsert import BulkInsert
from BenchmarkTargets.MultithreadingInsert import MultithreadedInsert
from Database.PostgresRepository import PostgresDB
import csv_generator

# Benchmarking setup
process = psutil.Process(os.getpid())
global_data = [i for i in range(10**6)]
OUTPUT_DIR = Path.cwd() / "output_data"
database = PostgresDB()
# Ensure parent directory exists
OUTPUT_DIR.mkdir(exist_ok=True)

DATA = Path.cwd() / "Data"
DATA.mkdir(exist_ok=True)


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
collected_stats = []

def get_pg_stats():
    with database.conn.cursor() as cur:
        # Step 1: Query statistics
            cur.execute("SELECT * FROM pg_stat_database WHERE datname = 'mydatabase';")
            row = cur.fetchone()
            if row:
                metrics = [
                    "xact_commit", "xact_rollback", "deadlocks",
                    "blk_read_time", "blk_write_time",
                    "session_time", "active_time", "idle_in_transaction_time",
                    "sessions", "sessions_abandoned", "sessions_fatal", "sessions_killed", 
                    "blks_read", "blks_hit", "blk_read_time", "blk_write_time"
                ]

                stat_snapshot = {metric: float(row.get(metric, 0.0)) for metric in metrics}
                collected_stats.append(stat_snapshot)

                # Reset stats after each round
                cur.execute("SELECT pg_stat_reset();")
                database.conn.commit()
    return

def compute_average_stats():
    """
    Compute average of collected stats.
    """
    if not collected_stats:
        print("No stats collected.")
        return {}

    num_snapshots = len(collected_stats)
    metrics = collected_stats[0].keys()
    totals = {metric: 0.0 for metric in metrics}

    for snapshot in collected_stats:
        for metric in metrics:
            totals[metric] += snapshot[metric]

    averages = {metric: totals[metric] / num_snapshots for metric in metrics}
    collected_stats.clear()
    return averages


def insert_extra_info(benchmark, averages):
    benchmark.extra_info["pg_avg_commits"] = f"{averages["xact_commit"]:.2f}"
    benchmark.extra_info["pg_avg_rollbacks"] = f"{averages["xact_rollback"]:.2f}"
    benchmark.extra_info["pg_avg_deadlocks"] = f"{averages["deadlocks"]:.2f}"

    benchmark.extra_info["blks_read"] = f"{averages["blks_read"]:.2f}"
    benchmark.extra_info["blks_hit"] = f"{averages["blks_hit"]:.2f}"
    benchmark.extra_info["blk_read_time"] = f"{averages["blk_read_time"]:.2f}"
    benchmark.extra_info["blk_write_time"] = f"{averages["blk_write_time"]:.2f}"


def test_sequential_insert(benchmark):
    def target():
        task = SequentialInsert(database=database, csv_file_path=DATA/ "data.csv")
        task.perform_task()
        get_pg_stats()


    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    print("Sequential insert. Sending a signal to stop!")
    with open("/app/control/stop_spammer.flag", "w") as f:
        f.write("STOP")

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)
    averages = compute_average_stats()

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


    insert_extra_info(benchmark=benchmark, averages=averages)

def test_sequential_insert_bulk(benchmark):
    def target():
        task = SequentialInsertBulk(database=database, csv_file_path=DATA/ "data.csv")
        task.perform_task()
        get_pg_stats()


    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    print("Sequential insert bulk. Sending a signal to stop!")
    with open("/app/control/stop_spammer.flag", "w") as f:
        f.write("STOP")

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)
    averages = compute_average_stats()

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


    insert_extra_info(benchmark=benchmark, averages=averages)


def test_bulk_insertion(benchmark):
    def target():
        task = BulkInsert(database=database, csv_file_path=DATA/ "data.csv")
        task.perform_task()
        get_pg_stats()

    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    print("bulk insertion. Sending a signal to stop!")
    with open("/app/control/stop_spammer.flag", "w") as f:
        f.write("STOP")

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)
    averages = compute_average_stats()

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


    insert_extra_info(benchmark=benchmark, averages=averages)


def test_multithreading(benchmark):
    def target():
        task = MultithreadedInsert(csv_file_path=DATA/ "data.csv")
        task.perform_task()
        get_pg_stats()

    mem_samples, stop = memory_sampler()
    result = benchmark(target)
    stop()

    print("multithreading. Sending a signal to stop!")
    with open("/app/control/stop_spammer.flag", "w") as f:
        f.write("STOP")

    avg_mem = sum(mem_samples) / len(mem_samples)
    peak_mem = max(mem_samples)
    averages = compute_average_stats()

    benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
    benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


    insert_extra_info(benchmark=benchmark, averages=averages)


# def test_list_comprehension(benchmark):
#     def target():
#         return [x * 2 for x in global_data]

#     mem_samples, stop = memory_sampler()
#     result = benchmark(target)
#     stop()

#     avg_mem = sum(mem_samples) / len(mem_samples)
#     peak_mem = max(mem_samples)

#     benchmark.extra_info["avg_rss_kb"] = f"{avg_mem:.2f}"
#     benchmark.extra_info["peak_rss_kb"] = f"{peak_mem:.2f}"


# Programmatically run the tests
def run_tests():
    # Run tests with pytest.main() and save the result to a JSON file
    result = pytest.main(
        [
            "-s", 
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
        fieldnames = ["name", "mean", "stddev", "ops", "avg_rss_kb", "peak_rss_kb", "pg_avg_commits", "pg_avg_rollbacks", "pg_avg_deadlocks", "blks_read", "blks_hit", "blk_read_time", "blk_write_time"]
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
                    "pg_avg_commits": info.get("pg_avg_commits", ""),
                    "pg_avg_rollbacks": info.get("pg_avg_rollbacks", ""),
                    "pg_avg_deadlocks": info.get("pg_avg_deadlocks", ""),

                    "blks_read": info.get("blks_read", ""),
                    "blks_hit": info.get("blks_hit", ""),
                    "blk_read_time": info.get("blk_read_time", ""),
                    "blk_write_time": info.get("blk_write_time", ""),
                }
            )

    print("Benchmark results saved to benchmark_results.csv")


# Call the function to run everything
if __name__ == "__main__":
    csv_generator.generate_csv(DATA/ "data.csv", num_rows=10_000)
    run_tests()
    database.close()
    sys.exit(0)
