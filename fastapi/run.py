import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "fastapi.main:app",
        host="0.0.0.0",
        port=8060,
        reload=True,
    )
