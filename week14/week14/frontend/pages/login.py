from dash import dcc, html
from dash import dcc, html, Input, Output, State, dash_table, no_update
import dash
from ...api.clients import post_login_api

from flask import session

layout = html.Div(
    [
        html.Div(
            [
                html.H1("Login Page"),
                dcc.Input(id="email", placeholder="Email", type="text"),
                html.Br(),
                dcc.Input(id="password", placeholder="Password", type="password"),
                html.Br(),
                html.Button("Log in", id="login-button", n_clicks=0, disabled=False),
                html.Div(id="login-outcome-message", children=None),
            ]
        )
    ]
)


@dash.callback(
    Output("login-button", "n_clicks"),
    Output("login-outcome-message", "children"),
    Output("url", "pathname"),
    Input("login-button", "n_clicks"),
    State("email", "value"),
    State("password", "value"),
)
def check_email_password(button, email, password):
    if button is None or button == 0:
        return dash.no_update, dash.no_update, dash.no_update

    # Attempt to log in to API
    token = post_login_api(email, password)

    if token is None:
        # Login to API failed
        return (
            0,
            html.P("Email or Password is Incorrect", style={"color": "red"}),
            dash.no_update,
        )

    # Login to API succeeded
    session["Authorization"] = f"Bearer {token}"
    return 0, html.P("Success!", style={"color": "lime"}), "/"
