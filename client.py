#!/usr/bin/python

import flask
from social.strategies.flask_strategy import FlaskStrategy
from oauth_backends import TwentyThreeAndMeOAuth2

PORT = 5000
API_SERVER_23ANDME = "api.23andme.com"
BASE_API_URL_23ANDME = "https://%s/" % API_SERVER_23ANDME
BASE_CLIENT_URL = 'http://localhost:%s/' % PORT
REDIR_URI_23ANDME = '%sreceive_code/' % BASE_CLIENT_URL
SCOPE_23ANDME = "basic"

fetch_23andme = flask.Flask(__name__)
fetch_23andme.config.from_pyfile('config_default.cfg')
fetch_23andme.config.from_pyfile('config_local.cfg')

# set the secret key:
fetch_23andme.secret_key = fetch_23andme.config['APP_SECRET_KEY']

# Madeleine: Pretty sure it's not supposed to look like this.
fetch_23andme.config['KEY'] = fetch_23andme.config['CLIENT_ID_23ANDME']
fetch_23andme.config['SECRET'] = fetch_23andme.config['CLIENT_SECRET_23ANDME']
fetch_23andme.config['SCOPE'] = fetch_23andme.config['SCOPE_23ANDME']
fetch_23andme.config['PIPELINE'] = (
    'social.pipeline.social_auth.social_details',
    )


@fetch_23andme.route('/')
def index():
    oauth_23andme = TwentyThreeAndMeOAuth2(redirect_uri=REDIR_URI_23ANDME,
                                           strategy=FlaskStrategy())
    return flask.render_template('index.html',
                                 auth_url=oauth_23andme.auth_url())


@fetch_23andme.route('/receive_code/')
def receive_23andme():
    oauth_23andme = TwentyThreeAndMeOAuth2(redirect_uri=REDIR_URI_23ANDME,
                                           strategy=FlaskStrategy())

    # Madeleine: auth_complete does a lot. I think it assumes I'm going to use
    # the social oauth database assumptions for storing data with user models.
    #
    # oauth_23andme.auth_complete()

    # Madeleine: The only alternative I see is the more basic method,
    # request_access_token, called by auth_complete(). But this is literally
    # just an alternate name for "get_json":
    # https://github.com/omab/python-social-auth/blob/master/social/backends/oauth.py#L343
    #
    # All the arguments request_access_token uses are defined and passed to it
    # by auth_complete(), not defined within the request_access_token method
    # itself (why??).
    token_data = oauth_23andme.request_access_token(
        oauth_23andme.ACCESS_TOKEN_URL,
        data=oauth_23andme.auth_complete_params(oauth_23andme.validate_state()),
        headers=oauth_23andme.auth_headers(),
        method=oauth_23andme.ACCESS_TOKEN_METHOD
    )
    user_data_basic = oauth_23andme.user_data_basic(token_data['access_token'])
    return flask.render_template('receive_code.html',
                                 retrieved_data=user_data_basic)

if __name__ == '__main__':
    print "A local client for the 23andme API is now initialized."
    fetch_23andme.run(debug=True, port=PORT)
