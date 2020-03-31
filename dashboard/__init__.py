import io
import json

import flask

from dashboard.database import get_latest_dictionary_url, get_dictionary_url

__VERSION__ = '20200329.01'

app = flask.Flask(__name__)


@app.route('/dictionary')
def dictionary():
    if 'project' not in flask.request.args:
        return flask.Response('Project name should be given.', status=400)
    if 'version' in flask.request.args:
        if not flask.request.args['version'].isnumeric():
            return flask.Response('Version should be a number.', status=400)
        result = get_dictionary_url(flask.request.args['project'],
                                    int(flask.request.args['version']))
    else:
        result = get_latest_dictionary_url(flask.request.args['project'])
    return flask.Response(json.dumps(result), status=200,
                          mimetype='application/json')


def run(port):
    app.run(host='0.0.0.0', port=port)

