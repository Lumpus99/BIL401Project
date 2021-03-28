import requests
from api.Enums import AuthorizationType
from urllib import parse
import preprocessor as p


def _jsonPath(data, path):
    current = data
    for i in path:
        if i in current:
            current = current[i]
        else:
            return ""
    return current


class ApiRequest:
    API_OBJECT = None
    AUTHORIZATION_OBJECT = None
    RESPONSE = None

    def __init__(self, api, auth=None):
        self.API_OBJECT = api
        self.AUTHORIZATION_OBJECT = auth

    def pullData(self):
        auth_header = {}
        api_key = ""
        if self.AUTHORIZATION_OBJECT is not None and self.AUTHORIZATION_OBJECT.AUTHORIZATION:
            if self.AUTHORIZATION_OBJECT.AUTHORIZATION_TYPE == AuthorizationType.OAuth:
                auth_header = "{} {}".format(self.AUTHORIZATION_OBJECT.AUTHORIZATION_FIELD,
                                             self.AUTHORIZATION_OBJECT.AUTHORIZATION_KEY)
            if self.AUTHORIZATION_OBJECT.AUTHORIZATION_TYPE == AuthorizationType.ApiKey:
                api_key = parse.urlencode(
                    {self.AUTHORIZATION_OBJECT.AUTHORIZATION_FIELD: self.AUTHORIZATION_OBJECT.AUTHORIZATION_KEY})

        headers = self.API_OBJECT.HEADER
        if auth_header != {}:
            headers["Authorization"] = auth_header

        full_url = self.API_OBJECT.URL + self.API_OBJECT.PATH + "?" + self.API_OBJECT.PARAMS + "&" + api_key

        response = requests.request(self.API_OBJECT.METHOD, full_url, headers=headers)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        self.RESPONSE = response.json()
        return self

    def parseData(self):
        return [p.clean(_jsonPath(a, self.API_OBJECT.TEXT_FIELD)) for a in
                _jsonPath(self.RESPONSE, self.API_OBJECT.ARRAY_FIELD)]

    def setAuth(self, auth):
        self.AUTHORIZATION_OBJECT = auth
