#!/usr/bin/env python3
"""
Start the Analytics Pipeline API server.
Usage: python scripts/start_api.py
"""
import os
import uvicorn
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

if __name__ == "__main__":
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8001"))
    
    print(f"Starting Analytics Pipeline API on {host}:{port}")
    uvicorn.run(
        "packages.api.main:app",
        host=host,
        port=port,
        reload=True
    )