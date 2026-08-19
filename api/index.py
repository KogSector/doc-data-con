from app.main import app

# Vercel entry point for Python ASGI applications
# The app variable must be named 'app', 'application', or 'handler'
application = app
app = app  # Also expose as 'app' for compatibility