import scrapy
import logging
from scrapy.crawler import CrawlerProcess
import json
from CONSTANT import *
from copy import deepcopy
from urlextract import URLExtract
import re
from pprint import pprint
from datetime import datetime



class Updator(scrapy.Spider):
    name = "pinterest_updator"
    resource_endpoint = 'https://www.pinterest.com:443/resource/UserResource/get/?'
    search_endpoint = 'https://www.pinterest.com/resource/BaseSearchResource/get/?source_url=/search/pins/?'
    activity_endpoint = 'https://www.pinterest.com:443/resource/UserActivityPinsResource/get/?'
    pin_endpoint = 'https://www.pinterest.com:443/resource/PinResource/get/?'
    link_extractor = URLExtract()
    email_regx = re.compile('[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,4}', re.IGNORECASE)


    def start_requests(self):
        for item in self.jsons:
            channelName = item.get("channelName")
            url = self.from_profile_parameters(self.resource_endpoint, channelName)
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"item":item})


    async def parse(self, response, **kwargs):
            item = kwargs.get("item")
            #########################
            data = json.loads(response.body)
            resource_data = data.get("resource_response", {}).get("data")
            channelName = item.get("channelName")
            channelURL = item.get('channelURL')
            metric_Subscribers = resource_data.get("follower_count") if resource_data.get("follower_count") and resource_data.get("follower_count") != -1 else 0
            metric_SubscribersOverTime = f"{datetime.now().strftime('%m/%d/%Y')}: {metric_Subscribers}, "
            metric_MonthlyViews = resource_data.get("profile_views") if resource_data.get("profile_views") and resource_data.get("profile_views") != -1 else 0
            metric_MonthyViewsOverTime = f"{datetime.now().strftime('%m/%d/%Y')}: {metric_MonthlyViews}, "
            channelDescription = resource_data.get("about")
            emailfromChannelDescription = ",".join(self.email_regx.findall(channelDescription))
            linksFromChannelDescription = ",".join(self.link_extractor.find_urls(channelDescription))
            emailFromChannel = resource_data.get("partner", {}).get("contact_email") if resource_data.get("partner", {}) else ''
            canMessage = resource_data.get("partner", {}).get("enable_profile_message") if resource_data.get("partner", {}) else False
            metric_ChannelNumberOfPosts = resource_data.get("pin_count")
            activity_data = await self.get_activity_data(channelName)
            if activity_data:
                metric_LastUploadDate = activity_data[0].get("created_at")
                emailFromLatestPostDescription = ",".join(self.email_regx.findall(activity_data[0].get("description")))
                resultant = await self.from_activity_data(activity_data)
                metric_Last30PostsDatePostedAndComments = resultant[0]
                metric_Last30PostsDatePostedAndReactions = resultant[1]
                text_30LatestPostsDescription = resultant[2]
                linksFrom30LatestPosts = resultant[3]
            else:
                metric_LastUploadDate = ''
                emailFromLatestPostDescription = ''
                metric_Last30PostsDatePostedAndComments = ''
                metric_Last30PostsDatePostedAndReactions = ''
                text_30LatestPostsDescription = ''
                linksFrom30LatestPosts = ''
            item.update({
                "channelName":channelName,
                "channelURL":channelURL,
                "metric_Subscribers":metric_Subscribers,
                "metric_MonthlyViews":metric_MonthlyViews,
                "channelDescription":channelDescription,
                "canMessage":canMessage,
                "metric_ChannelNumberOfPosts":metric_ChannelNumberOfPosts,
                "metric_LastUploadDate":metric_LastUploadDate,
                "metric_Last30PostsDatePosted&NumberofComments":metric_Last30PostsDatePostedAndComments,
                "metric_Last30PostsDatePosted&Reactions":metric_Last30PostsDatePostedAndReactions,
                "text_30LatestPostsDescription":text_30LatestPostsDescription,
                "linksfrom30LatestPosts":linksFrom30LatestPosts,
                "lastUpdated": datetime.now().strftime("%m/%d/%Y")
            })
            item['linksfromChannelDescription'] += linksFromChannelDescription
            item['emailfromChannelDescription'] += emailfromChannelDescription
            item['emailfromChannel'] += emailFromChannel
            item['emailfromLatestPostDescr'] += emailFromLatestPostDescription
            item['metric_SubscribersOverTime'] += metric_SubscribersOverTime
            item['metric_MonthlyViewsOverTime'] += metric_MonthyViewsOverTime
            yield item


    async def from_activity_data(self, activity_data):
        metric_Last30PostsDatePostedAndComments = ''
        metric_Last30PostsDatePostedAndReactions = ''
        text_30LatestPostsDescription = ''
        linksFrom30LatestPosts = ''
        for n, d in enumerate(activity_data, start=1):
            id = d.get('id')
            pin_data = await self.get_pin_data(id)
            date_ = self.get_date(pin_data.get('created_at'))
            comments = pin_data.get("aggregated_pin_data", {}).get('comment_count')
            reactions = sum(pin_data.get('reaction_counts').values())
            description = pin_data.get('description').strip() or pin_data.get("seo_description")
            links = ",".join(self.link_extractor.find_urls(description))
            metric_Last30PostsDatePostedAndComments += f"{date_}:{comments},"
            metric_Last30PostsDatePostedAndReactions += f"{date_}:{reactions}"
            text_30LatestPostsDescription += f"{description} "
            linksFrom30LatestPosts += links
            if n == 30:
                break
        return (
            metric_Last30PostsDatePostedAndComments,
            metric_Last30PostsDatePostedAndReactions,
            text_30LatestPostsDescription,
            linksFrom30LatestPosts,
        )

    async def get_url(self, url):
        response = await self.crawler.engine.download(scrapy.Request(url))
        data = json.loads(response.body)
        return data

    # async def get_resource_data(self, channelName):
    #     url = self.from_profile_parameters(self.resource_endpoint, channelName)
    #     response = await self.get_url(url)
    #     resouce_data = response.get("resource_response", {}).get("data")
    #     return resouce_data

    async def get_activity_data(self, channelName):
        url = self.from_activity_parameters(self.activity_endpoint, channelName)
        response = await self.get_url(url)
        activity_data = response.get("resource_response", {}).get("data")
        return activity_data

    async def get_pin_data(self, id):
        url = self.from_pin_parameters(self.pin_endpoint, id)
        response = await self.get_url(url)
        pin_data = response.get("resource_response", {}).get("data")
        return pin_data

    @staticmethod
    def build_params(keyword):
        params = deepcopy(search_parameters)
        params['q'] = keyword
        params['data']['options']['query'] = keyword
        return params

    @staticmethod
    def from_profile_parameters(resource_endpoint, channelName):
        profile_params = deepcopy(profile_parameters)
        profile_params['source_url'] = f"/{channelName}/"
        profile_params['data']['options']['username'] = channelName
        url = resource_endpoint + "source_url=" + profile_params['source_url'] + '&' + "data=" + json.dumps(profile_params['data'])
        return url

    @staticmethod
    def from_activity_parameters(activity_endpoint, channelName):
        activity_params = deepcopy(activity_parameters)
        activity_params['source_url'] = f"/{channelName}/_created/"
        activity_params['data']['options']['username'] = channelName
        url = activity_endpoint + "source_url=" + activity_params['source_url'] + '&' + "data=" + json.dumps(activity_params['data'])
        return url

    @staticmethod
    def from_pin_parameters(pin_endpoint, id):
        pin_params = deepcopy(pin_parameters)
        pin_params['source_url'] = f"/{id}/_created/"
        pin_params['data']['options']['id'] = id
        url = pin_endpoint + "source_url=" + pin_params['source_url'] + '&' + "data=" + json.dumps(pin_params['data'])
        return url

    @staticmethod
    def get_date(date_string):
        datetime_obj = datetime.strptime(date_string, '%a, %d %b %Y %H:%M:%S %z')
        date_only_string = datetime_obj.strftime('%d/%m/%Y')
        return date_only_string

    @staticmethod
    def clean(item):
        cleaned_item = {}
        for k,v in item.items():
            if isinstance(v, str):
                cleaned_item[k] = v.strip()
            else:
                cleaned_item[k] = v
        return cleaned_item


crawler = CrawlerProcess(settings={
    "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko)",
    "DEFAULT_REQUEST_HEADERS": {
        'Accept': 'application/json, text/javascript, */*, q=0.01',
        'Accept-Language': 'en',
        "X-Requested-With": "XMLHttpRequest",
        "Host": "www.pinterest.com"
    },
    "LOG_LEVEL": logging.DEBUG,
    "LOG_ENABLED": True,
    "DOWNLOAD_DELAY": 0.5,
    "CONCURRENT_REQUESTS": 8,
    "HTTPCACHE_ENABLED": True,
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
})
# check CONSTANT.py for api_response
crawler.crawl(Updator, jsons=api_response)
crawler.start()


