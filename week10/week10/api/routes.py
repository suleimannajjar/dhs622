from fastapi import APIRouter, Body, HTTPException
from starlette.requests import Request

from ..utilities.logic import (
    get_names_of_seed_lists,
    get_seed_list_preview,
    get_seed_channel_metadata,
    get_birth_chart_data,
    make_forward_network,
)

router = APIRouter()

# TODO: implement the /time_series_chart_api route handler,
#  imitating the syntax for /birth_chart
# TODO: implement the /domain_network route handler,
#  imitating the syntax for /forward_network

@router.get("/seed_list_names")
async def seed_list_names_api(request: Request):
    return {"data": get_names_of_seed_lists()}


@router.post("/seed_list_preview")
async def seed_list_preview_api(
    request: Request, seed_list_names: list = Body(embed=True)
):
    return {"data": get_seed_list_preview(seed_list_names)}


@router.post("/seed_metadata_full")
async def seed_metadata_full_api(
    request: Request, seed_list_names: list[str] = Body(embed=True)
):
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