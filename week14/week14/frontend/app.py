from dash import Dash, dcc, html, Input, Output
import dash

import secrets
from .pages import welcome, analyze as analyze, login
from flask import session
from ..api.clients import get_me_api
from ..utilities.security_logic import parse_token_from_flask

app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server
server.secret_key = secrets.token_hex()

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content"),
    ]
)


@dash.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if not session:
        return login.layout

    if not get_me_api(parse_token_from_flask()):
        return login.layout

    if pathname == "/":
        return welcome.layout
    elif pathname == "/analyze":
        return analyze.layout
    else:
        return "404"


def run():
    app.run(host='0.0.0.0', port=8050, debug=True, use_reloader=False)

if __name__ == "__main__":
    run()
