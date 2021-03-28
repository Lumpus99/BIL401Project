from enum import Enum


class AuthorizationType(Enum):
    OAuth = 0
    ApiKey = 1


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
