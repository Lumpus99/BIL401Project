class Authorization:
    AUTHORIZATION = False
    AUTHORIZATION_KEY = None
    AUTHORIZATION_TYPE = None  # API KEY vs OAuth
    AUTHORIZATION_FIELD = None

    def __init__(self, authorization=True):
        self.AUTHORIZATION = authorization

    def key(self, key):
        self.AUTHORIZATION_KEY = key
        return self

    def type(self, auth_type):
        self.AUTHORIZATION_TYPE = auth_type
        return self

    def field(self, field):
        self.AUTHORIZATION_FIELD = field
        return self
