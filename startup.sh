#!/bin/bash
set -eu

# With Oryx remote build the app is extracted to /tmp/<id> and this script is
# invoked from there, while /home/site/wwwroot holds only output.tar.zst.
# Only fall back to wwwroot when the package is not in the current directory.
if [ ! -f "./app/main.py" ] && [ -f "/home/site/wwwroot/app/main.py" ]; then
  cd /home/site/wwwroot
fi

export PYTHONPATH="$(pwd)${PYTHONPATH:+:$PYTHONPATH}"

if [ -d "./.python_packages/lib/site-packages" ]; then
  export PYTHONPATH="./.python_packages/lib/site-packages:${PYTHONPATH}"
fi

if [ -d "./antenv/bin" ]; then
  export PATH="./antenv/bin:${PATH}"
fi

PORT="${PORT:-8000}"
WORKERS="${WEB_CONCURRENCY:-1}"

echo "Starting ReadyToBuild from $(pwd) on 0.0.0.0:${PORT} workers=${WORKERS}"

if python -c "import gunicorn" 2>/dev/null; then
  exec python -m gunicorn app.main:app \
    --bind "0.0.0.0:${PORT}" \
    --workers "${WORKERS}" \
    --worker-class uvicorn.workers.UvicornWorker \
    --timeout 120 \
    --access-logfile - \
    --error-logfile -
fi

echo "gunicorn missing; falling back to uvicorn"
exec python -m uvicorn app.main:app --host 0.0.0.0 --port "${PORT}"
