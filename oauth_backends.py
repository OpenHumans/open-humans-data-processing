import json
from social.backends.oauth import BaseOAuth2
from social.p3 import urlencode

class TwentyThreeAndMeOAuth2(BaseOAuth2):
    """23andme OAuth authentication backend"""
    name = '23andme'
    ID_KEY = 'id'
    AUTHORIZATION_URL = 'https://api.23andme.com/authorize'
    ACCESS_TOKEN_URL = 'https://api.23andme.com/token'
    ACCESS_TOKEN_METHOD = 'POST'
    SCOPE_SEPARATOR = ' '
    REDIRECT_STATE = False
    STATE_PARAMETER = False

    def do_auth(self, access_token, response=None, *args, **kwargs):
        """Finish auth process once access token is received."""
        print "In 23andmeOAuth do_auth"
        response = response or {}
        data = self.user_data(access_token)
        data['access_token'] = response.get('access_token')
        data['refresh_token'] = response.get('refresh_token')
        data['expires'] = response.get('expires_in')
        kwargs.update({'backend': self, 'response': data})
        return self.strategy.authenticate(*args, **kwargs)

    def get_user_id(self, details, response):
        """Return a unique ID for the current user, by default from server
        response."""
        return response.get(self.ID_KEY)

    def get_user_details(self, response):
        """In 23andme, basic scope returns none of these fields."""
        return {
            'username': '',
            'email': '',
            'fullname': '',
            'first_name': '',
            'last_name': ''}

    def user_data_basic(self, access_token, *args, **kwargs):
        """Loads basic user data from 23andme

        Scope required: basic

        This retrieves the following account data:
        'id'             id for this account
        'profiles'       list of profiles in the account

        Each profile has:
            'id'         id for this profile
            'genotyped'  whether a profile has been genotyped
            'services'   list of services that profile has access to
        """
        assert 'basic' in self.get_scope(), "'basic' scope required"
        params = {'services': 'true'}
        headers = {'Authorization': 'Bearer %s' % access_token}
        return self.get_json('https://api.23andme.com/1/user/',
                             params=params, headers=headers)

    def user_data(self, access_token, *args, **kwargs):
        return self.user_data_basic(access_token, *args, **kwargs)

    def auth_complete_params(self, *args, **kwargs):
        params = super(TwentyThreeAndMeOAuth2,
                       self).auth_complete_params(*args, **kwargs)
        params.update(self.get_scope_argument())
        return params
