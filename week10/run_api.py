import uvicorn
from fastapi import FastAPI
from week10.config import api_host, api_port
from week10.api.routes import router

app = FastAPI()
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host=api_host, port=api_port)