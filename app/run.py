import sys, os, subprocess
# Launch server.py as a subprocess to avoid .pyc cache issues
site_packages = os.path.expanduser("~") + r"\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\site-packages"
env = os.environ.copy()
env["PYTHONPATH"] = site_packages
env["PYTHONDONTWRITEBYTECODE"] = "1"
script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.py")
subprocess.run([sys.executable, "-B", "-X", "utf8", script], env=env, cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
