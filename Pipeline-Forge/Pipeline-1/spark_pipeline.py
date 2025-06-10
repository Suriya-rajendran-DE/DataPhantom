import subprocess
import os

GIT_REPO_URL = "https://github.com/your-repo/data-pipeline"

# Clone repo and fetch latest scripts
os.system(f"git clone {GIT_REPO_URL} pipeline_repo")
os.system("cd pipeline_repo && git pull")

# Run Hive scripts
subprocess.run(["hive", "-f", "pipeline_repo/create_hive_tables.hql"])

# Run transformation script
subprocess.run(["spark-submit", "--master", "local", "pipeline_repo/transform_data.py"])

# Run Data Quality check script
subprocess.run(["spark-submit", "--master", "local", "pipeline_repo/dq_check.py"])