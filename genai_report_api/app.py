from fastapi import FastAPI
from fastapi.responses import JSONResponse
import os

app = FastAPI()

@app.get("/")
async def read_root():
    """
    A simple endpoint to confirm the deployment was successful.
    """
    return JSONResponse(
        content={
            "status": "Deployment Smoke Test Successful! The new code is live.",
            "message": "If you can see this, the deployment pipeline is working correctly."
        }
    )

if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        reload=False
    )