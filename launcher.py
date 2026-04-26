"""
CryptoAgent launcher — starts the FastAPI server then opens the browser.
Bundled into CryptoAgent.exe via PyInstaller.
"""
import os
import sys
import time
import subprocess
import webbrowser
import urllib.request

PORT = int(os.getenv("PORT", 8000))
URL  = f"http://localhost:{PORT}"


def _find_python() -> str:
    """Return the Python executable to use for the server."""
    # When running as a frozen exe, we can't reuse sys.executable for the server
    # (it would just re-launch the launcher). Fall back to whatever python is in PATH.
    for candidate in ("python", "python3"):
        try:
            subprocess.run([candidate, "--version"], capture_output=True, check=True)
            return candidate
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass
    return sys.executable  # last resort


def _wait_for_server(timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(URL, timeout=1)
            return True
        except Exception:
            time.sleep(0.5)
    return False


def main():
    # Resolve the directory where server.py lives.
    # sys._MEIPASS is set by PyInstaller when running as a bundled exe.
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    server_script = os.path.join(base, "server.py")

    python = _find_python()

    print(f"Starting CryptoAgent server on {URL} …")
    proc = subprocess.Popen(
        [python, server_script],
        cwd=base,
        # Keep the console window hidden when launched via double-click
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )

    print("Waiting for server to be ready …")
    if _wait_for_server(30):
        print(f"Server ready — opening {URL}")
        webbrowser.open(URL)
    else:
        print("Server did not start in time. Check server.log for errors.")

    # Keep running so the server process stays alive
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()


if __name__ == "__main__":
    main()
