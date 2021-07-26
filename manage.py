#!/usr/bin/env python3
import sys
import os


def print_green(s):
    GREEN = "\033[92m"
    ENDC = "\033[0m"
    print(f"{GREEN}{s}{ENDC}")


def run_command(cmd):
    print_green(cmd)
    os.system(cmd)


def lock(extra_args):
    """
    Write requirements-lock.txt and requirements-test-lock.txt
    """
    requirements_files = {
        "requirements.txt": "requirements-lock.txt",
        "requirements-test.txt": "requirements-test-lock.txt",
    }

    for src, locked in requirements_files.items():
        command = f"""\
        docker run -v $(pwd):/app python:3.9 \
            /bin/bash -c "pip install -qqq -r /app/{src} && pip freeze" > {locked}
        """
        run_command(command)

def main():
    command = sys.argv[1]
    command_func = globals()[command]
    extra_args = " " + " ".join(sys.argv[2:])
    command_func(extra_args)


if __name__ == "__main__":
    main()
