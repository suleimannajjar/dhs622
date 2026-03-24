from dash import dcc, html, Input, Output, State, dash_table, no_update
import dash
import plotly.express as px

import dash_cytoscape as cyto

import pandas as pd

from datetime import date


from ...api.clients_unauth import (
    get_seed_list_names_api,
    post_seed_list_preview_api,
    post_seed_metadata_full_api,
    post_birth_chart_api,
    post_time_series_chart_api,
    post_message_table_data_api,
    post_make_forward_network_api,
    post_make_domain_network_api,
    post_domain_table_data_api,
    post_single_channel_metadata_api,
)

from networkx.classes.digraph import DiGraph

def map_communities_to_colors(G: DiGraph) -> dict:
    unique_communities = list(set([G.nodes()[node]["cluster"] for node in G.nodes()]))
    rows = []
    for community in unique_communities:
        num_nodes = len(
            [node for node in G.nodes() if G.nodes()[node]["cluster"] == community]
        )
        rows.append((community, num_nodes))
    df = pd.DataFrame(rows, columns=["cluster", "num_nodes"])
    df.sort_values("num_nodes", ascending=False, inplace=True)
    df.reset_index(inplace=True, drop=True)

    colors = ["red", "blue", "green", "yellow", "purple", "pink", "orange"]

    communities_to_colors = {}
    for i in range(0, df.shape[0]):
        if i < len(colors):
            communities_to_colors[df.loc[i, "cluster"]] = colors[i]
        else:
            communities_to_colors[df.loc[i, "cluster"]] = "grey"

    return communities_to_colors

def make_cytoscape_stylesheet(
    my_nodes: list[dict], my_edges: list[dict], hovered_node: dict = None
) -> list[dict]:
    of_interest_opacity = 1
    not_of_interest_opacity = 0.2

    min_node_strength = min([node["data"]["size"] for node in my_nodes])
    max_node_strength = max([node["data"]["size"] for node in my_nodes])
    min_edge_weight = min([edge["data"]["weight"] for edge in my_edges])
    max_edge_weight = max([edge["data"]["weight"] for edge in my_edges])

    my_stylesheet = [
        {
            "selector": "node",
            "style": {
                "content": "data(label)",
                "color": "black",
                "text-valign": "center",
                "text-halign": "center",
                "width": f"mapData(size, {min_node_strength}, {max_node_strength}, 1, 50)",
                "height": f"mapData(size, {min_node_strength}, {max_node_strength}, 1, 50)",
                "font-size": f"mapData(size, {min_node_strength}, {max_node_strength}, 1, 50)",
            },
        },
        {
            "selector": "edge",
            "style": {
                "width": f"mapData(weight, {min_edge_weight}, {max_edge_weight}, 0.1, 5)",
                "curve-style": "bezier",
            },
        },
    ]

    if hovered_node is not None:
        edges_of_interest = [
            edge
            for edge in my_edges
            if edge["data"]["source"] == hovered_node["id"]
            or edge["data"]["target"] == hovered_node["id"]
        ]
        node_ids_of_interest = list(
            set(
                [edge["data"]["source"] for edge in edges_of_interest]
                + [edge["data"]["target"] for edge in edges_of_interest]
            )
        )
    else:
        edges_of_interest = my_edges
        node_ids_of_interest = [my_node["data"]["id"] for my_node in my_nodes]

    my_stylesheet += [
        {
            "selector": 'node[id = "{}"]'.format(node["data"]["id"]),
            "style": {
                "opacity": of_interest_opacity
                if node["data"]["id"] in node_ids_of_interest
                else not_of_interest_opacity,
                "background-color": node["data"]["color"],
            },
        }
        for node in my_nodes
    ]

    my_stylesheet += [
        {
            "selector": 'edge[id = "{}"]'.format(edge["data"]["id"]),
            "style": {
                "opacity": of_interest_opacity
                if edge in edges_of_interest
                else not_of_interest_opacity,
                "line-color": edge["data"]["color"],
            },
        }
        for edge in my_edges
    ]

    return my_stylesheet

def make_cytoscape_elements(
    G: DiGraph, weight_var: str = "count_1", label_var: str = "channel_name"
) -> tuple[list[dict], list[dict]]:
    community_to_colors_mapper = map_communities_to_colors(G)

    # Create Cytoscape elements:
    my_nodes = [
        {
            "data": {
                "type": "node",
                "id": str(node),
                "label": str(G.nodes()[node][label_var])
                if label_var in G.nodes()[node].keys()
                else "",
                "size": G.nodes()[node]["in_strength"],
                "color": community_to_colors_mapper[G.nodes()[node]["cluster"]],
            }
        }
        for node in G.nodes()
    ]

    # my_edges = [
    #     {
    #         "data": {
    #             "type": "edge",
    #             "id": f"{source}-{target}",
    #             "source": source,
    #             "target": target,
    #             "weight": weight,
    #             "color": community_to_colors_mapper[
    #                 G.nodes()[source]["cluster"]
    #             ],
    #         }
    #     }
    #     for (source, target, weight) in G.edges.data(weight_var, default=0)
    #     if weight > 0
    # ]

    my_edges = [
        {
            "data": {
                "type": "edge",
                "id": f"{source}-{target}",
                "source": str(source),
                "target": str(target),
                "weight": G.edges()[(source, target)][weight_var],
                "color": community_to_colors_mapper[G.nodes()[source]["cluster"]],
            }
        }
        for (source, target) in G.edges()
    ]

    return my_nodes, my_edges

layout = html.Div(
    [
        html.H1("Analysis page"),
        dcc.Dropdown(
            id="seed-list-menu",
            options=[],
            placeholder="Select your seed list(s)",
            style={"color": "black"},
            value=None,
            disabled=False,
            multi=True,
        ),
        html.Div(id="seed-list-menu-feedback"),
    ]
)


@dash.callback(Output("seed-list-menu", "options"), Input("seed-list-menu", "options"))
def populate_menu_with_seed_lists(my_options: list[dict]) -> list[dict]:
    # Query the database for all seed lists
    return [
        {"label": my_seed_list, "value": my_seed_list}
        for my_seed_list in get_seed_list_names_api()
    ]


@dash.callback(
    Output("seed-list-menu-feedback", "children"), Input("seed-list-menu", "value")
)
def display_seed_list_preview_and_date_range_picker(my_seed_lists: list[str]) -> html:
    if not isinstance(my_seed_lists, list) or len(my_seed_lists) == 0:
        return None
    records = post_seed_list_preview_api(my_seed_lists)

    return html.Div(
        [
            dash_table.DataTable(
                data=records,
                columns=[{"name": key, "id": key} for key in records[0].keys()],
                style_cell={"textAlign": "left"},
                page_current=0,
                page_size=10,
                export_format="csv",
                sort_action="native",
            ),
            html.Div(
                [
                    dcc.DatePickerRange(
                        id="my-date-picker-range",
                        min_date_allowed=date(2013, 1, 1),
                        max_date_allowed=date(2099, 12, 31),
                        initial_visible_month=date.today(),
                        start_date=None,
                        end_date=None,
                        disabled=False,
                    ),
                    html.Div(id="my-date-picker-range-feedback"),
                ]
            ),
        ]
    )


@dash.callback(
    Output("my-date-picker-range-feedback", "children"),
    Input("my-date-picker-range", "start_date"),
    Input("my-date-picker-range", "end_date"),
)
def display_analyze_button(start_date: str, end_date: str) -> html:
    if start_date is None or end_date is None:
        return None
    return html.Div(
        [
            html.Button("Analyze", id="analyze-button", n_clicks=0, disabled=False),
            html.Div(id="analysis-container"),
        ]
    )


@dash.callback(
    Output("analysis-container", "children"), Input("analyze-button", "n_clicks")
)
def specify_analysis_container(analyze_button_clicks):
    if analyze_button_clicks == 0:
        return no_update

    return html.Div(
        [
            # analytic components:
            html.Div(id="seed-channel-metadata-table"),
            html.Div(id="forward-network-container"),
            html.Div(id="domain-network-container"),
            html.Div(id="domain-table-container"),
            html.Div(id="birth-chart-container"),
            dcc.RadioItems(
                id="birth-chart-unit",
                options=[
                    {"label": "Years", "value": "year"},
                    {"label": "Months", "value": "month"},
                    {"label": "Weeks", "value": "week"},
                    {"label": "Days", "value": "day"},
                    {"label": "Hours", "value": "hour"},
                    {"label": "Minutes", "value": "minute"},
                ],
                value="month",
            ),
            html.Br(),
            html.Div(id="time-series-chart-container"),
            dcc.RadioItems(
                id="time-series-chart-unit",
                options=[
                    {"label": "Years", "value": "year"},
                    {"label": "Months", "value": "month"},
                    {"label": "Weeks", "value": "week"},
                    {"label": "Days", "value": "day"},
                    {"label": "Hours", "value": "hour"},
                    {"label": "Minutes", "value": "minute"},
                ],
                value="day",
            ),
            html.Br(),
            html.Div(id="message-table-container"),
        ]
    )


@dash.callback(
    Output("seed-channel-metadata-table", "children"),
    Input("seed-channel-metadata-table", "children"),
    State("seed-list-menu", "value"),
)
def print_channel_metadata_table(
    seed_table_children: html, seed_list_names: list[str]
) -> html:
    records = post_seed_metadata_full_api(seed_list_names)

    return html.Div(
        [
            dash_table.DataTable(
                data=records,
                columns=[{"name": key, "id": key} for key in records[0].keys()],
                style_cell={"textAlign": "left"},
                page_current=0,
                page_size=10,
                export_format="csv",
                sort_action="native",
            )
        ]
    )


@dash.callback(
    Output("birth-chart-container", "children"),
    Input("birth-chart-unit", "value"),
    State("seed-list-menu", "value"),
)
def render_birth_chart(birth_chart_unit: str, seed_list_names: list[str]) -> html:
    fig = px.bar(
        post_birth_chart_api(birth_chart_unit, seed_list_names),
        x="creation_dt",
        y="count",
    )
    fig.update_layout(
        title_text="Seed channel birthdates",
        xaxis_title_text="Date/time",
        yaxis_title_text="Number of births",
        template="plotly_dark",
    )
    return dcc.Graph(id="seed-channel-birth-chart", figure=fig)


@dash.callback(
    Output("time-series-chart-container", "children"),
    Input("time-series-chart-unit", "value"),
    State("seed-list-menu", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def render_time_series_chart(
    time_series_chart_unit: str,
    seed_list_names: list[str],
    start_date: str,
    end_date: str,
) -> html:
    df = pd.DataFrame.from_records(
        post_time_series_chart_api(
            time_series_chart_unit, seed_list_names, start_date, end_date,
        )
    )

    fig = px.line(df, x="message_dt", y="count")
    fig.update_layout(
        title_text=f"Seed channel message counts ({df['count'].sum()} total)",
        xaxis_title_text="Date/time",
        yaxis_title_text="Number of Messages",
        template="plotly_dark",
    )
    return html.Div([dcc.Graph(id="time-series-chart", figure=fig)])


@dash.callback(
    Output("message-table-container", "children"),
    Input("message-table-container", "children"),
    State("seed-list-menu", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def render_message_table_callback(
    message_table_children: html,
    seed_list_names: list[str],
    start_date: str,
    end_date: str,
) -> html:
    records = post_message_table_data_api(seed_list_names, start_date, end_date, 1000)

    return html.Div(
        [
            dash_table.DataTable(
                id="message-table",
                data=records,
                columns=[
                    {"name": key, "id": key, "type": "text", "presentation": "markdown"}
                    if key == "url"
                    else {"id": key, "name": key}
                    for key in records[0].keys()
                ],
                style_cell={"textAlign": "left"},
                style_data={
                    "whiteSpace": "normal",
                    "height": "auto",
                },
                page_current=0,
                page_size=10,
                export_format="csv",
                sort_action="native",
            ),
        ]
    )


@dash.callback(
    Output("forward-network-container", "children"),
    Input("forward-network-container", "children"),
    State("seed-list-menu", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def render_forward_network(
    message_table_children, seed_list_names, start_date, end_date
):
    G = post_make_forward_network_api(seed_list_names, start_date, end_date, 800)
    my_nodes, my_edges = make_cytoscape_elements(G, "weight")
    my_stylesheet = make_cytoscape_stylesheet(my_nodes, my_edges)

    return html.Div(
        [
            html.P(
                f"Forwarding network has {len(G.nodes())} nodes and {len(G.edges())} edges"
            ),
            cyto.Cytoscape(
                id="forward-network",
                layout={"name": "cose"},
                style={"width": "90%", "height": "800px"},
                elements=my_nodes + my_edges,
                stylesheet=my_stylesheet,
                responsive=True,
            ),
            html.Br(),
            html.Div(id="tapNode-feedback"),
        ]
    )


@dash.callback(
    Output("domain-network-container", "children"),
    Input("domain-network-container", "children"),
    State("seed-list-menu", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def render_domain_network(
    domain_network_container, seed_list_names, start_date, end_date
):
    B = post_make_domain_network_api(seed_list_names, start_date, end_date, 800)
    my_nodes, my_edges = make_cytoscape_elements(B, "weight", "label")
    my_stylesheet = make_cytoscape_stylesheet(my_nodes, my_edges)

    return html.Div(
        [
            html.P(
                f"Domain network has {len(B.nodes())} nodes and {len(B.edges())} edges"
            ),
            cyto.Cytoscape(
                id="domain-network",
                layout={"name": "cose"},
                style={"width": "90%", "height": "800px"},
                elements=my_nodes + my_edges,
                stylesheet=my_stylesheet,
                responsive=True,
            ),
            html.Br(),
            html.Div(id="domain-tapNode-feedback"),
        ]
    )


@dash.callback(
    Output("domain-table-container", "children"),
    Input("domain-table-container", "children"),
    State("seed-list-menu", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def render_domain_table(domain_table_container, seed_list_names, start_date, end_date):
    domain_records = post_domain_table_data_api(seed_list_names, start_date, end_date)

    return html.Div(
        [
            dash_table.DataTable(
                data=domain_records,
                columns=[
                    {"name": key, "id": key, "type": "text", "presentation": "markdown"}
                    if key == "domain"
                    else {"id": key, "name": key}
                    for key in domain_records[0].keys()
                ],
                style_cell={"textAlign": "left"},
                style_data={
                    "whiteSpace": "normal",
                    "height": "auto",
                },
                page_current=0,
                page_size=10,
                export_format="csv",
                sort_action="native",
            )
        ]
    )


@dash.callback(
    Output("forward-network", "stylesheet"),
    Input("forward-network", "mouseoverNodeData"),
    State("forward-network", "elements"),
)
def change_transparency_forward_network(hovered_node, my_elements):
    if not hovered_node:
        return no_update

    return make_cytoscape_stylesheet(
        [x for x in my_elements if x["data"]["type"] == "node"],
        [x for x in my_elements if x["data"]["type"] == "edge"],
        hovered_node,
    )


@dash.callback(
    Output("domain-network", "stylesheet"),
    Input("domain-network", "mouseoverNodeData"),
    State("domain-network", "elements"),
)
def change_transparency_domain_network(hovered_node, my_elements):
    if not hovered_node:
        return no_update

    return make_cytoscape_stylesheet(
        [x for x in my_elements if x["data"]["type"] == "node"],
        [x for x in my_elements if x["data"]["type"] == "edge"],
        hovered_node,
    )


@dash.callback(
    Output("tapNode-feedback", "children"),
    Input("forward-network", "tapNodeData"),
    State("seed-list-menu", "value"),
    State("my-date-picker-range", "start_date"),
    State("my-date-picker-range", "end_date"),
)
def print_information_after_click(node_data, seed_list_names, start_date, end_date):
    if not node_data:
        return no_update

    metadata_df = pd.DataFrame.from_records(
        post_single_channel_metadata_api(node_data["id"])
    )

    if metadata_df.empty:
        return html.Div(
            html.P(
                f"The node you clicked on has ID {node_data['id']}. "
                f"The database contains no further information on it."
            )
        )

    return html.Div(
        [
            html.P(f"Here is some metadata on the channel you clicked on:"),
            dash_table.DataTable(
                data=metadata_df.to_dict("records"),
                columns=[{"name": i, "id": i} for i in list(metadata_df)],
                style_cell={"textAlign": "left"},
                style_data={
                    "whiteSpace": "normal",
                    "height": "auto",
                },
                page_current=0,
                page_size=10,
                export_format="csv",
                sort_action="native",
            ),
        ]
    )
