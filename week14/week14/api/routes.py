from fastapi import APIRouter, Body, HTTPException
from starlette.requests import Request

from ..utilities.logic import (
    get_names_of_seed_lists,
    get_seed_list_preview,
    get_seed_channel_metadata,
    get_birth_chart_data,
    make_forward_network,
    get_time_series_chart_data,
    render_message_table,
    make_domain_table,
    make_domain_network
)

from ..utilities.security_logic import check_credentials, create_jwt, verify_token, parse_token_from_starlette

router = APIRouter()

@router.post("/login")
async def login_api(
    request: Request, email: str = Body(embed=True), password: str = Body(embed=True)
):
    # (1) check if this email is in our database; if not, return HTTP 401
    result = check_credentials(email)
    if result is None:
        raise HTTPException(status_code=401, detail="ah ah ah, you didn't say the magic word!")
    # (2) if so, check if the password is correct; if not, return HTTP 401
    if password != result["password"]:
        raise HTTPException(status_code=401, detail="ah ah ah, you didn't say the magic word!")
    # (3) if so, create an unguessable token and return it
    token = create_jwt(email)
    return {"token": token}

@router.get("/me", response_description="Logged in user data")
# async def me(request: Request, user_email=Depends(oauth2_scheme)): ALTERNATIVE WAY TO DO AUTH
async def me(request: Request):
    email = verify_token(parse_token_from_starlette(request))
    return {"data": email}

@router.get("/seed_list_names")
async def seed_list_names_api(request: Request):
    email = verify_token(parse_token_from_starlette(request))
    return {"data": get_names_of_seed_lists()}


@router.post("/seed_list_preview")
async def seed_list_preview_api(
    request: Request, seed_list_names: list = Body(embed=True)
):
    email = verify_token(parse_token_from_starlette(request))
    return {"data": get_seed_list_preview(seed_list_names)}


@router.post("/seed_metadata_full")
async def seed_metadata_full_api(
    request: Request, seed_list_names: list[str] = Body(embed=True)
):
    email = verify_token(parse_token_from_starlette(request))
    records = get_seed_channel_metadata(seed_list_names)

    # Convert datetime to string
    for record in records:
        record["channel_birthdate"] = str(record["channel_birthdate"])

    return {"data": records}


@router.post("/birth_chart")
async def birth_chart_api(
    request: Request,
    unit: str = Body(embed=True),
    seed_list_names: list = Body(embed=True),
):
    email = verify_token(parse_token_from_starlette(request))
    records = get_birth_chart_data(unit, seed_list_names)

    records = [
        {
            "count": record["count"],
            "creation_dt": record["creation_dt"].strftime("%Y-%m-%d %H:%M:%SZ"),
        }
        for record in records
    ]

    return {"data": records}


@router.post("/forward_network")
async def make_forward_network_api(
    request: Request,
    start_date: str = Body(embed=True),
    end_date: str = Body(embed=True),
    seed_list_names: list = Body(embed=True),
    network_max_size: int = Body(embed=True),
):
    email = verify_token(parse_token_from_starlette(request))
    if network_max_size == 0:
        network_max_size = None

    B = make_forward_network(
        seed_list_names,
        start_date,
        end_date,
        network_max_size,
    )
    return {
        "data": {
            "nodes": [B.nodes()[node] for node in B.nodes()],
            "edges": [
                (str(edge[0]), str(edge[1]), int(B.edges()[edge]["count_1"]))
                for edge in B.edges()
            ],
        }
    }

@router.post("/time_series_chart")
async def time_series_chart_api(
    request: Request,
    unit: str = Body(embed=True),
    seed_list_names: list = Body(embed=True),
    start_date: str = Body(embed=True),
    end_date: str = Body(embed=True),
):
    email = verify_token(parse_token_from_starlette(request))
    records = get_time_series_chart_data(start_date, end_date, unit, seed_list_names)

    records = [
        {
            "count": record["count"],
            "message_dt": record["message_dt"].strftime("%Y-%m-%d %H:%M:%SZ"),
        }
        for record in records
    ]

    return {"data": records}


@router.post("/message_table")
async def render_message_table_api(
    request: Request,
    start_date: str = Body(embed=True),
    end_date: str = Body(embed=True),
    seed_list_names: list = Body(embed=True),
    the_limit: int = Body(embed=True),
):
    email = verify_token(parse_token_from_starlette(request))
    if the_limit == 0:
        the_limit = None
    records = render_message_table(start_date, end_date, seed_list_names, the_limit)

    for record in records:
        record["message_datetime"] = record["message_datetime"].strftime(
            "%Y-%m-%d %H:%M:%SZ"
        )

    return {"data": records}


@router.post("/domain_table")
async def render_domain_table_api(
    request: Request,
    start_date: str = Body(embed=True),
    end_date: str = Body(embed=True),
    seed_list_names: list = Body(embed=True),
):
    email = verify_token(parse_token_from_starlette(request))
    return {"data": make_domain_table(seed_list_names, start_date, end_date)}


@router.post("/domain_network")
async def make_domain_network_api(
    request: Request,
    start_date: str = Body(embed=True),
    end_date: str = Body(embed=True),
    seed_list_names: list = Body(embed=True),
    network_max_size: int = Body(embed=True),
):
    email = verify_token(parse_token_from_starlette(request))
    if network_max_size == 0:
        network_max_size = None

    B = make_domain_network(
        seed_list_names,
        start_date,
        end_date,
        network_max_size,
    )
    return {
        "data": {
            "nodes": [B.nodes()[node] for node in B.nodes()],
            "edges": [
                (str(edge[0]), str(edge[1]), int(B.edges()[edge]["weight"]))
                for edge in B.edges()
            ],
        }
    }