from fastapi import FastAPI

from domain.tsum20 import tsum20


app = FastAPI()


app.mount("/", tsum20.tsumApp)



@app.get('/hello')
def hello():
    return "hello"