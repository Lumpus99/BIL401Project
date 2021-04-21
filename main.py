from api import ApiObject, Authorization, ApiRequest
from api.Enums import AuthorizationType as At
import json


def twitterQueryExample(query, TWITTER_BEARER_TOKEN, num=10):
    try:
        api = ApiObject.Api().text_field(("data",), ("text",))

        auth = Authorization.Authorization()

        api = api.url("https://api.twitter.com").path("/2/tweets/search/recent") \
            .params({'query': query, 'max_results': num})

        auth = auth.type(At.OAuth).key(TWITTER_BEARER_TOKEN).field("Bearer")

        request = ApiRequest.ApiRequest(api, auth).pullData().parseData()
        twitter_array = []
        for i in request:  # CSV Twitter index(0,1,2..), text="kendisi", "target"=0
            print("> Twitter", i)
            twitter_array.append(i)
        return twitter_array
    except:
        print("Cannot find word(s) for Twitter!!!")


def youtubeQueryExample(query, YOUTUBE_API_KEY, num=5):
    api = ApiObject.Api().text_field(("items",), ("snippet", "topLevelComment", "snippet", "textOriginal"))

    auth = Authorization.Authorization()
    try:
        api = api.url(" https://youtube.googleapis.com").path("/youtube/v3/commentThreads") \
            .params({'part': 'snippet', 'maxResults': num, "videoId": query})
        auth = auth.type(At.ApiKey).key(YOUTUBE_API_KEY).field("key")
        request2 = ApiRequest.ApiRequest(api, auth).pullData().parseData()
        youtube_array = []
        for i in request2:  # CSV dosyasi SÜTUNLAR= index(0,1,2..), text="kendisi", "target"=0
            print("> Youtube: ", i)
            youtube_array.append(i)
        return youtube_array
    except:
        print("Cannot find Youtube the link!!!")


if __name__ == '__main__':
    # DO NOT COMMIT API KEYS!!!

    with open('api_tokens.json') as f:
        tokens = json.load(f)

    print("Getting data from youtube...")
    youtubeQueryExample("Nj-bM6OqnE0", tokens['youtube_api_key'])
    print("===================================================================")
    print("Getting data from twitter...")
    twitterQueryExample("France", tokens['twitter_bearer_token'])
