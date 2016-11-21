# import libraries
import StringIO, requests, json, ConfigParser, logging, time, urllib
import pandas as pd
from datetime import datetime
from simple_salesforce import Salesforce
from optparse import OptionParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler(str(time.strftime("%d_%m_%Y")) +"_sfdc_enrichment" + ".log")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

Config = ConfigParser.ConfigParser()
Config.read("sfdc_enrichment.ini")
Config.sections()
logger.debug("Sections: %s" % Config.sections())

def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                logger.debug("skip: %s" % option)
        except:
            logger.warn("exception on %s!" % option)
            dict1[option] = None
    return dict1

class Google_API_Enrich(object):
        #class to pull in a data set and iterate through enriching data in each row
    def __init__(self, api_info):
        """api_info is a dictionary that contains information related to the specific looker instance"""
        self.g_key = api_info['google_key']
        self.maps_url = api_info['google_maps_url']

    def lat_long_request(self,address_qs):
        try:
            r = requests.get(self.maps_url + '?address={0}&key={1}'.format(address_qs,self.g_key))
            return json.loads(r.text)
        except requests.exceptions.RequestException as e:
            logger.error(e)

class SFDC_API_Mod(object):
    def __init__(self, api_info):
        self.client_secret = api_info['sfdc_client_secret']
        self.client_id = api_info['sfdc_client_id']
        self.sfdc_pass = api_info['sfdc_password']
        self.api_token = api_info['sfdc_api_token']
        self.username = api_info['sfdc_username']

    def login(self):
        try:
            sf = Salesforce(username=self.username, password=self.sfdc_pass, security_token=self.api_token)
            self.sf = sf
        except:
            logger.error('Issue with SF Account Login')

    def lat_long_update(self,account_api_id,latitude,longitude):
        try:
            #print account_api_id + str({'BillingLatitude': latitude, 'BillingLongitude': longitude})
            self.sf.Account.update(account_api_id,{'BillingLatitude': latitude, 'BillingLongitude': longitude})
        except:
            logger.error('Issue with SF Account Update')


            def customers_locations(customers_loc,sfdc_api,google_api):
                for index,row in customers_loc.iterrows():
                    address_qs = urllib.quote_plus(row['Company Formatted Address'])
                    #print address_qs
                    results = google_api.lat_long_request(address_qs)
                    try:
                        result_filter = results["results"][0] #just get first result in returned set
                        sfdc_api.lat_long_update(row['Account ID'],result_filter["geometry"]['location']['lat'],result_filter["geometry"]['location']['lng'])
                    except:
                        logger.warn("failed look up of %s" % row['Company Formatted Address'])
                        logger.error('issue with result pull')
