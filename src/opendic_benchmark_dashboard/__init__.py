import os
import subprocess
import sys


def main() -> None:
    print("Hello from opendic-benchmark-dashboard!")


def run_streamlit_app() -> None:
    """Run the Streamlit app"""
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "streamlit_app.py")
    subprocess.run([sys.executable, "-m", "streamlit", "run", file_path])
