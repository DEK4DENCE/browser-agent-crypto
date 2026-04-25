@echo off
echo ═══════════════════════════════════════════
echo  CRYPTO AGENT — Setup
echo ═══════════════════════════════════════════
echo.

echo [1/4] Creating virtual environment...
python -m venv .venv
call .venv\Scripts\activate.bat

echo [2/4] Installing Python dependencies...
pip install -r requirements.txt

echo [3/4] Installing Playwright Chromium browser...
playwright install chromium
playwright install-deps chromium 2>nul

echo [4/4] Creating .env file...
if not exist .env (
    copy .env.example .env
    echo.
    echo  !! IMPORTANT: Edit .env and add your ANTHROPIC_API_KEY !!
    echo.
) else (
    echo .env already exists, skipping.
)

echo.
echo ═══════════════════════════════════════════
echo  Setup complete!
echo.
echo  Run the web UI:     python main.py --server
echo  Run CLI:            python main.py "should I ape HYPE?"
echo  Run agent swarm:    python main.py --swarm BTC ETH HYPE
echo ═══════════════════════════════════════════
pause
