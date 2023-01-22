import subprocess

result = subprocess.run(
    ["python", "-c", "'import setuptools; setuptools.setup()'", "--version"],
    text=True,
    stdout=subprocess.PIPE,
)
print(result.stdout)
print(result.stderr)
