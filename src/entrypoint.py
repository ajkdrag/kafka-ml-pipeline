from fastapi import FastAPI, status, Response
from fastapi.responses import JSONResponse


app = FastAPI()


@app.get("/health")
async def health_check():
    pass


@app.post("/train")
async def train(item: Item, response: Response):
    pass


@app.post("/predict")
async def predict(item: Item, response: Response):
    pass
