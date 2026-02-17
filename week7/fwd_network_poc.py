import os
from week7.config import HOME_DIR
from week7.utilities.logic1 import make_forward_network
import networkx as nx


def run():
    OUTPUT_DIR = os.path.join(HOME_DIR, "PycharmProjects", "smo", "week7", "data")
    SEED_LIST_NAME = "russian_disinfo"
    START_DATE = "2022-01-01"
    END_DATE = "2023-01-01"

    G = make_forward_network([SEED_LIST_NAME], START_DATE, END_DATE)

    nx.write_graphml(G, os.path.join(OUTPUT_DIR, f"{SEED_LIST_NAME}_fwd_network.graphml"))

if __name__ == '__main__':
    run()