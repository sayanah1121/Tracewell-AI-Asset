import os
import sys

JOBS = os.path.join(os.path.dirname(__file__), "..", "..", "spark-jobs")
sys.path.insert(0, os.path.join(JOBS, "investigate"))
sys.path.insert(0, os.path.join(JOBS, "catalog"))
