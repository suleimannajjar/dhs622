import requests
from urllib.parse import urljoin
from datetime import datetime
import pandas as pd
import networkx as nx

from ..config import api_base

# TODO: implement the post_time_series_chart_api() client function,
#  imitating the syntax for post_birth_chart_api()

# TODO: implement the post_make_domain_network_api() client function,
#  imitating the syntax for post_make_forward_network_api()

def format_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%SZ")

def get_seed_list_names_api() -> list[str]:
    resp = requests.get(
        urljoin(api_base, "seed_list_names")
    )
    resp.raise_for_status()
    return resp.json()["data"]

def post_seed_list_preview_api(seed_list_names: list[str]) -> list[dict]:
    resp = requests.post(
        urljoin(api_base, "seed_list_preview"),
        json={"seed_list_names": seed_list_names},
    )
    resp.raise_for_status()

    return resp.json()["data"]


def post_birth_chart_api(
    unit: str, seed_list_names: list[str]
) -> list[dict]:
    resp = requests.post(
        urljoin(api_base, "birth_chart"),
        json={"seed_list_names": seed_list_names, "unit": unit},
    )
    resp.raise_for_status()

    records = resp.json()["data"]
    for record in records:
        record["creation_dt"] = format_date(record["creation_dt"])
    return records


def post_make_forward_network_api(
    seed_list_names, start_date, end_date, network_max_size: int
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