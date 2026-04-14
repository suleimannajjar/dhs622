import uvicorn
from fastapi import FastAPI
from week14.config import api_host, api_port
from week14.api.routes import router

app = FastAPI()
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=api_port)