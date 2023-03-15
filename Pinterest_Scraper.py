import scrapy
import logging
from scrapy.crawler import CrawlerProcess
import json
from CONSTANT import search_parameters, profile_parameters, activity_parameters
from copy import deepcopy
from scrapy import signals
from datetime import datetime


class Scraper(scrapy.Spider):
    name = 'pinterest_spider'
    resource_endpoint = 'https://www.pinterest.com:443/resource/UserResource/get/?'
    search_endpoint = 'https://www.pinterest.com/resource/BaseSearchResource/get/?'
    activity_endpoint = 'https://www.pinterest.com:443/resource/UserActivityPinsResource/get/?'
    channels = set()
    result_counter = 0
    batch = []
    batch_size = 50


    def start_requests(self):
        self.maxResults = self.maxResults
        for keyword in self.keywords:
            params = self.build_params(keyword.get("key"))
            url = self.search_endpoint + f'source_url={params.get("source_url")}&data={json.dumps(params["data"])}'
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={"keyword":keyword.get("key"), 
                                                                      "uid": keyword.get("idOutRequest"), 
                                                                      "min_subs":keyword.get("minimumNumberofSubscribers"),
                                                                      "cutoffdays": keyword.get("lastUploadCutoffDate")
                                                                      })


    async def parse(self, response, keyword, uid, min_subs, cutoffdays):
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
                    resource_data = await self.get_resource(channelName)
                    metric_MonthlyViews = resource_data.get("profile_views")
                    channelDescription = resource_data.get("about")
                    # checkup
                    allow = await self.allowed(metric_Subscribers, min_subs, cutoffdays, channelName)
                    if not allow:    
                        continue
                    item = dict(
                        idOutRequest=uid,
                        keyword=keyword,
                        idChannel=idChannel,
                        channelName=channelName,
                        channelURL=channelURL,
                        metric_Subscribers=metric_Subscribers,
                        metric_MonthlyViews=metric_MonthlyViews if metric_MonthlyViews != -1 else 0,
                        channelDescription=channelDescription
                    )
                    if self.result_counter < self.maxResults:
                        self.result_counter +=1
                        self.batch.append(item)
                        print(f" [+] Scraped: [{self.result_counter}]")
                        if self.result_counter % self.batch_size == 0:
                            self.save_to_db(self.batch)
                    else:
                        break
            bookmark = data.get('resource_response', {}).get("bookmark")
            if (self.result_counter < self.maxResults) and bookmark:
                params = self.build_params(keyword)
                params['data']['options'].update({"bookmarks": [bookmark]})
                url = self.search_endpoint + f'source_url={params.get("source_url")}&data={json.dumps(params["data"])}&_={data.get("request_identifier")}'
                yield scrapy.Request(url, callback=self.parse, cb_kwargs={"keyword": keyword, "uid": uid})
            else:
                self.save_to_db(self.batch)


    async def get_url(self, url):
        response = await self.crawler.engine.download(scrapy.Request(url))
        data = json.loads(response.body)
        return data


    async def get_resource(self, channelName):
        url = self.from_profile_parameters(self.resource_endpoint, channelName)
        response = await self.get_url(url)
        data = response.get("resource_response", {}).get("data")
        return data


    async def get_activity_data(self, channelName):
        url = self.from_activity_parameters(self.activity_endpoint, channelName)
        response = await self.get_url(url)
        activity_data = response.get("resource_response", {}).get("data")
        return activity_data


    async def allowed(self, metric_Subscribers, min_subs, cutoffdays, channelName):
        if metric_Subscribers <= min_subs:
            return False
        else:
            activity_data = await self.get_activity_data(channelName)
            metric_LastUploadDate = activity_data[0].get("created_at") if activity_data else None
            date_obj = datetime.strptime(metric_LastUploadDate, '%a, %d %b %Y %H:%M:%S %z') if metric_LastUploadDate else None
            if date_obj:
                date_str = date_obj.strftime("%Y-%m-%d")
                today_str = datetime.now().strftime("%Y-%m-%d")
                age = self.days_between(date_str, today_str)
                if age >= cutoffdays:
                    return False
        return True


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


    @staticmethod
    def from_activity_parameters(activity_endpoint, channelName):
        activity_params = deepcopy(activity_parameters)
        activity_params['source_url'] = f"/{channelName}/_created/"
        activity_params['data']['options']['username'] = channelName
        url = activity_endpoint + "source_url=" + activity_params['source_url'] + '&' + "data=" + json.dumps(activity_params['data'])
        return url


    @staticmethod
    def days_between(date1, date2):
        date1_obj = datetime.strptime(date1, '%Y-%m-%d')
        date2_obj = datetime.strptime(date2, '%Y-%m-%d')
        delta = date2_obj - date1_obj
        return delta.days
    

    def save_to_db(self, batch):
        if batch:
            # insert to db, batch is 50 records
            # code here
            self.batch = []




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
    "HTTPCACHE_ENABLED": False,
    "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",

})
# lastUploadCutoffDate: the number of days after channel last post
crawler.crawl(Scraper, keywords=[{"key": 'Batman', "idOutRequest": 1, "minimumNumberofSubscribers":10, "lastUploadCutoffDate": 100}], maxResults=20)
crawler.start()
