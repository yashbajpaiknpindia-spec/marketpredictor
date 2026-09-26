"""Also applies when Render's existing start command is just gunicorn app:app."""
import os

bind = "0.0.0.0:" + os.environ.get("PORT", "10000")
workers = 1
worker_class = "gthread"
threads = 4
timeout = 120
preload_app = False
# Recycling a worker mid-export terminates its background thread.
max_requests = 0
