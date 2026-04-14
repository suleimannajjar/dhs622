import requests
from urllib.parse import urljoin
from datetime import datetime
import pandas as pd
import networkx as nx

from ..config import api_base


def post_login_api(email: str, password: str) -> str:
    resp = requests.post(
        urljoin(api_base, "login"),
        json={"email": email, "password": password},
    )
    resp.raise_for_status()

    return resp.json()["token"]

def get_me_api(token: str) -> list[str]:
    resp = requests.get(urljoin(api_base, "me"), headers=get_auth_header(token))
    resp.raise_for_status()
    return resp.json()["data"]

def get_auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

def format_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%SZ")

def get_seed_list_names_api(token: str) -> list[str]:
    resp = requests.get(
        urljoin(api_base, "seed_list_names"),
        headers=get_auth_header(token)
    )
    resp.raise_for_status()
    return resp.json()["data"]

def post_seed_list_preview_api(seed_list_names: list[str], token: str) -> list[dict]:
    resp = requests.post(
        urljoin(api_base, "seed_list_preview"),
        json={"seed_list_names": seed_list_names},
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    return resp.json()["data"]

def post_seed_metadata_full_api(seed_list_names: list[str], token: str) -> list[dict]:
    resp = requests.post(
        urljoin(api_base, "seed_metadata_full"),
        json={"seed_list_names": seed_list_names},
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    records = resp.json()["data"]
    for record in records:
        record["channel_birthdate"] = str(
            record["channel_birthdate"]
        )
    return records


def post_birth_chart_api(
    unit: str, seed_list_names: list[str], token: str
) -> list[dict]:
    resp = requests.post(
        urljoin(api_base, "birth_chart"),
        json={"seed_list_names": seed_list_names, "unit": unit},
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    records = resp.json()["data"]
    for record in records:
        record["creation_dt"] = format_date(record["creation_dt"])
    return records


def post_make_forward_network_api(
    seed_list_names, start_date, end_date, network_max_size: int, token: str
):
    if network_max_size == None:
        network_max_size = 0

    resp = requests.post(
        urljoin(api_base, "forward_network"),
        json={
            "start_date": start_date,
            "end_date": end_date,
            "seed_list_names": seed_list_names,
            "network_max_size": network_max_size,
        },
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    data = resp.json()["data"]
    nodes = data["nodes"]
    edges = data["edges"]
    edges_df = pd.DataFrame(edges, columns=["source", "target", "weight"])

    B = nx.DiGraph()
    B.clear()
    B.add_weighted_edges_from(
        edges_df.loc[:, ["source", "target", "weight"]].values, weight="weight"
    )
    B.add_nodes_from([(node["channel_id"], node) for node in nodes])
    print(f"graph has {len(B.nodes())} nodes and {len(B.edges())} edges")

    return B


def post_time_series_chart_api(
    unit: str, seed_list_names: list[str], start_date: str, end_date: str, token: str) -> list[dict]:
    resp = requests.post(
        urljoin(api_base, "time_series_chart"),
        json={
            "seed_list_names": seed_list_names,
            "unit": unit,
            "start_date": start_date,
            "end_date": end_date,
        },
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    records = resp.json()["data"]
    for record in records:
        record["message_dt"] = format_date(record["message_dt"])
    return records


def post_message_table_data_api(
    seed_list_names: list[str],
    start_date: str,
    end_date: str,
    the_limit: int,
    token: str,
):
    if the_limit is None:
        the_limit = 0
    resp = requests.post(
        urljoin(api_base, "message_table"),
        json={
            "start_date": start_date,
            "end_date": end_date,
            "seed_list_names": seed_list_names,
            "the_limit": the_limit,
        },
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    records = resp.json()["data"]
    for record in records:
        record["message_datetime"] = format_date(record["message_datetime"])

    return records


def post_domain_table_data_api(
    seed_list_names: list[str], start_date: str, end_date: str, token: str):
    resp = requests.post(
        urljoin(api_base, "domain_table"),
        json={
            "start_date": start_date,
            "end_date": end_date,
            "seed_list_names": seed_list_names,
        },
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    return resp.json()["data"]


def post_single_channel_metadata_api(channel_id: str, token: str):
    resp = requests.post(
        urljoin(api_base, "single_channel_metadata"),
        json={
            "channel_id": channel_id,
        },
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    return resp.json()["data"]


def post_make_domain_network_api(
    seed_list_names, start_date, end_date, network_max_size: int, token: str):
    if network_max_size == None:
        network_max_size = 0

    resp = requests.post(
        urljoin(api_base, "domain_network"),
        json={
            "start_date": start_date,
            "end_date": end_date,
            "seed_list_names": seed_list_names,
            "network_max_size": network_max_size,
        },
        headers=get_auth_header(token)
    )
    resp.raise_for_status()

    data = resp.json()["data"]
    nodes = data["nodes"]
    edges = data["edges"]
    edges_df = pd.DataFrame(edges, columns=["source", "target", "weight"])

    B = nx.DiGraph()
    B.clear()
    B.add_weighted_edges_from(
        edges_df.loc[:, ["source", "target", "weight"]].values, weight="weight"
    )
    B.add_nodes_from([(node["label"], node) for node in nodes])
    print(f"graph has {len(B.nodes())} nodes and {len(B.edges())} edges")

    return B