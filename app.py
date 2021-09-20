from db import *
from fastapi import Security, Depends, FastAPI, HTTPException
from starlette.responses import JSONResponse
from starlette import status

app = FastAPI()
session = create_connection()


@app.get("/user")
async def retrieve_user(email: str):

    resp = get_user(session, email)
    return JSONResponse(resp, status_code=status.HTTP_200_OK)


@app.post("/signup")
async def create_user(email: str, lastname: str, age: int, city: str, firstname: str):

    set_user(session, lastname, age, city, email, firstname)
    return JSONResponse("user creaed", status_code=status.HTTP_200_OK)


@app.post("/update_age")
async def update_user_age(email: str, new_age: int):

    update_user(session, new_age, email)
    return JSONResponse("updated", status_code=status.HTTP_200_OK)


@app.get("/remove")
async def remove_user(email: str):

    delete_user(session, email)
    return JSONResponse("user dleted", status_code=status.HTTP_200_OK)
