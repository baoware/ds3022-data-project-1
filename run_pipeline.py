import logging
import os
import subprocess
import sys
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='run_pipeline.log'
)
logger = logging.getLogger(__name__)

# run everything from the repo folder so relative paths (emissions.duckdb, data/, logs) resolve
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

# each stage writes its own log file (load.log, clean.log, transform.log, analysis.log)
STAGES = ['load.py', 'clean.py', 'transform.py', 'analysis.py']


# function to run one command and stop the pipeline if it fails
def run_stage(name, command, cwd=REPO_DIR):
    print(f"\n=== {name} ===", flush=True)
    logger.info(f"Starting {name}: {' '.join(command)}")
    start = time.time()
    result = subprocess.run(command, cwd=cwd)
    elapsed = time.time() - start
    if result.returncode != 0:
        raise RuntimeError(f"{name} failed with exit code {result.returncode} after {elapsed:.1f}s")
    logger.info(f"Finished {name} in {elapsed:.1f}s")
    print(f"--- {name} finished in {elapsed:.1f}s", flush=True)


# main function: run load -> clean -> transform -> analysis
def run_pipeline():
    try:
        for script in STAGES:
            run_stage(script, [sys.executable, script])

        print("\nPipeline completed successfully")
        logger.info("Pipeline completed successfully")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    run_pipeline()
