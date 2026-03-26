from .db import insert_data_into_credentials_table, fetch_credentials_if_exist
from jose import jwt, ExpiredSignatureError, JWTError
import secrets
import datetime
from fastapi import HTTPException
from starlette.requests import Request

SECRET_KEY = secrets.token_hex(20)
ALGORITHM = "HS256"

TOKEN_LIFETIME_MINUTES = 60 * 24 * 7

def add_credentials(credentials: list[dict]) -> None:
    return insert_data_into_credentials_table(credentials)

def check_credentials(email: str) -> dict|None:
    return fetch_credentials_if_exist(email)

def create_jwt(email: str) -> str:
    expire = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        minutes=TOKEN_LIFETIME_MINUTES
    )
    jwt_data = {"sub": email, "iat": datetime.datetime.now(datetime.UTC), "exp": expire}
    encoded_jwt = jwt.encode(jwt_data, key=SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def parse_token_from_starlette(request: Request):
    bearer_string = request.headers.get("Authorization", None)
    if bearer_string is None:
        raise HTTPException(
            status_code=401, detail="Authorization header missing / empty"
        )
    if not bearer_string.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization header malformed")
    return bearer_string.split(" ")[1]

def verify_token(token: str):
    """decode token"""
    try:
        payload = jwt.decode(token, key=SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except ExpiredSignatureError as e:
        raise HTTPException(status_code=401, detail="Token expired") from e
    except JWTError as e:
        raise HTTPException(status_code=401, detail=str(e)) from e