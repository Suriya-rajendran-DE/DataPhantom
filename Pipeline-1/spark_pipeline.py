import subprocess
import os

GIT_REPO_URL = "https://github.com/Suriya-rajendran-DE/DataPhantom.git"

# Clone repo if not already cloned
if not os.path.exists("pipeline_repo"):
    subprocess.run(["git", "clone", GIT_REPO_URL, "pipeline_repo"])

# Pull latest changes
subprocess.run(["git", "-C", "pipeline_repo", "pull"])

# Run Hive scripts
subprocess.run(["hive", "-f", "pipeline_repo/create_hive_tables.hql"], cwd=os.getcwd())

# Run transformation script
subprocess.run(["spark-submit", "--master", "local[*]", "pipeline_repo/transform_data.py"])

# Run Data Quality check script
subprocess.run(["spark-submit", "--master", "local[*]", "pipeline_repo/dq_check.py"])