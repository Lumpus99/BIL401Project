from urllib import parse


class Api:
    URL = ""
    PATH = ""
    PARAMS = ""
    METHOD = "GET"
    ARRAY_FIELD = ()
    TEXT_FIELD = ()
    HEADER = {}
    AUTHORIZATION_OBJECT = None

    def __init__(self):
        pass

    def url(self, url):
        self.URL = url
        return self

    def path(self, path):
        self.PATH = path
        return self

    def params(self, params):
        self.PARAMS = parse.urlencode(params)
        return self

    def text_field(self, array_field, text_field):
        self.ARRAY_FIELD = array_field
        self.TEXT_FIELD = text_field
        return self

    def header(self, header):
        self.HEADER = header
        return self

    def authorization(self, authorization):
        self.AUTHORIZATION_OBJECT = authorization
        return self
