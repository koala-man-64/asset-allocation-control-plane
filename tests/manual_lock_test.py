
import sys
import time
import subprocess

from core import core as mdc

def run_worker(job_name, sleep_time):
    print(f"[Worker] Attempting to acquire lock for {job_name}...")
    try:
        with mdc.JobLock(job_name):
            print(f"[Worker] Lock acquired. Sleeping for {sleep_time}s...")
            time.sleep(sleep_time)
            print("[Worker] Waking up and releasing lock.")
    except SystemExit:
        print("[Worker] Failed to acquire lock (SystemExit).")
    except Exception as e:
        print(f"[Worker] Error: {e}")

if __name__ == "__main__":
    job_name = "test-verification-lock"
    
    # 1. Start a subprocess that holds the lock for 10 seconds
    print("[Main] Starting worker process...")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        run_worker(job_name, 20)
        sys.exit(0)

    # Main process
    p = subprocess.Popen([sys.executable, __file__, "--worker"])
    
    # Wait for worker to start and acquire lock
    time.sleep(10) 
    
    print("[Main] Worker should be holding lock now. Attempting to acquire same lock...")
    try:
        with mdc.JobLock(job_name):
            print("[Main] ERROR: I acquired the lock! Concurrency check FAILED.")
    except SystemExit:
        print("[Main] SUCCESS: SystemExit caught. Lock successfully prevented concurrent run.")
    except Exception as e:
        print(f"[Main] Unexpected error: {e}")

    p.wait()
    print("[Main] Done.")
