import requests
from urllib.parse import urljoin
api_base = "http://127.0.0.1:8000"


def get_seed_list_names_api() -> list[str]|None:
    resp = requests.get(urljoin(api_base, "seed_list_names"))
    if resp.status_code != 200:
        return None
    return resp.json()["data"]


if __name__ == "__main__":
    print(get_seed_list_names_api())
