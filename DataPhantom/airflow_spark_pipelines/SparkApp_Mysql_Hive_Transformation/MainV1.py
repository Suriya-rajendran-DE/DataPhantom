

To properly capture syntax-level errors, modify your run_script() function like this:


def run_script(script_name):
    script_path = os.path.join(BASE_PATH, script_name)
    print(f"Running script: {script_path}")

    try:
        result = subprocess.run(
            ["python3", script_path],
            check=True,
            capture_output=True,
            text=True
        )
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)

    except subprocess.CalledProcessError as e:
        print(f"Error while executing {script_name}")
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)
        raise RuntimeError(f"Script {script_name} failed.") from e

    except Exception as e:
        print(f"Unexpected error in {script_name}: {str(e)}")
        raise