import argparse

import toml


def read_pyproject_dependencies(filepath):
    try:
        with open(filepath, "r") as file:
            toml_data = toml.load(file)
    except FileNotFoundError:
        print(f"Error: {filepath} not found.")
        return None

    requirements = toml_data["project"]["dependencies"]
    return requirements


def minimize_requirements(requirements):
    minimized = []
    for req in requirements:
        if ">=" in req:
            minimized.append(req.replace(">=", "=="))
        elif "==" in req:
            minimized.append(req)
    return minimized


def write_to_file(requirements):
    with open("tests/minimal_requirements.txt", "w") as file:
        for req in requirements:
            file.write(req + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Minimize dependencies from pyproject.toml",
    )
    parser.add_argument(
        "filepath",
        nargs="?",
        default="pyproject.toml",
        help="Path to pyproject.toml file",
    )
    args = parser.parse_args()

    requirements = read_pyproject_dependencies(args.filepath)
    requirements = minimize_requirements(requirements)
    write_to_file(requirements)
