import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import the FastAPI application
from app.main import app

# Expose it for Vercel
app = app
application = app