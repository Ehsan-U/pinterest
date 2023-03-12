import random

import scrapy
import logging
from scrapy.crawler import CrawlerProcess
import json
from CONSTANT import search_parameters, profile_parameters
from copy import deepcopy
from urllib.parse import urlencode


class Scraper(scrapy.Spider):
    name = 'pinterest_spider'
    resource_endpoint = 'https://www.pinterest.com:443/resource/UserResource/get/?'
    search_endpoint = 'https://www.pinterest.com/resource/BaseSearchResource/get/?'
    channels = set()
    result_counter = 0
    page_no=0

    def start_requests(self):
        self.maxResults = self.maxResults
        for keyword in self.keywords:
            params = self.build_params(keyword.get("key"))
            url = self.search_endpoint + f'source_url={params.get("source_url")}' + '&data=' + json.dumps(params['data'])
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"keyword":keyword.get("key"), "uid": keyword.get("idOutRequest")})

    async def parse(self, response, keyword, uid):
        print(f" \n{self.page_no}\n")
        data = json.loads(response.body)
        results = data.get("resource_response", {}).get("data", {}).get("results")
        if results:
            for result in results:
                idChannel = result.get("pinner", {}).get("id")
                if idChannel not in self.channels:
                    self.channels.add(idChannel)
                    channelName = result.get("pinner", {}).get("username")
                    channelURL = f'https://www.pinterest.com/{channelName}/'
                    metric_Subscribers = result.get("pinner", {}).get("follower_count")
                    url = self.from_profile_parameters(self.resource_endpoint, channelName)
                    response = await self.get_url(url)
                    data_helper = response.get("resource_response", {}).get("data")
                    metric_MonthlyViews = data_helper.get("profile_views")
                    channelDescritpion = data_helper.get("about")
                    item = dict(
                        idOutRequest=uid,
                        keyword=keyword,
                        idChannel=idChannel,
                        channelName=channelName,
                        channelURL=channelURL,
                        metric_Subscribers=metric_Subscribers,
                        metric_MonthlyViews=metric_MonthlyViews if metric_MonthlyViews != -1 else 0,
                        channelDescritpion=channelDescritpion
                    )
                    if self.result_counter < self.maxResults:
                        self.result_counter +=1
                        print(f"[{self.result_counter}] >> {item}")
                        # yield item
                    else:
                        break
        if (self.result_counter < self.maxResults):
            self.page_no+=1
            bookmark = data.get('resource_response').get("bookmark")
            params = self.build_params(keyword)
            params['data']['options'].update({"bookmarks": [bookmark]})
            url = self.search_endpoint + f'source_url={params.get("source_url")}' + '&data=' + json.dumps(params['data']) + '&_=' + data.get("request_identifier")
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"keyword": keyword, "uid": uid})


    async def get_url(self, url):
        response = await self.crawler.engine.download(scrapy.Request(url))
        data = json.loads(response.body)
        return data

    @staticmethod
    def build_params(keyword):
        params = deepcopy(search_parameters)
        params['source_url'] = f'/search/pins/?q={keyword}'
        params['data']['options']['query'] = keyword
        return params

    @staticmethod
    def from_profile_parameters(resource_endpoint, channelName):
        profile_params = deepcopy(profile_parameters)
        profile_params['source_url'] = f"/{channelName}/"
        profile_params['data']['options']['username'] = channelName
        url = resource_endpoint + "source_url=" + profile_params['source_url'] + '&' + "data=" + json.dumps(profile_params['data'])
        return url



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
    "DOWNLOAD_DELAY": 3,
    "CONCURRENT_REQUESTS": 8,
    "HTTPCACHE_ENABLED": True,
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
})
crawler.crawl(Scraper, keywords=[{"key": 'Rumi', "idOutRequest": 1}], maxResults=50)
crawler.start()
