# Copyright (c) 2015 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import urllib
import urllib2
from urllib2 import Request, URLError

from poster.encode import multipart_encode
from api_config import SparkApiConfig
import util
import requests
import os
import subprocess
import json


class SparkApi:
    def __init__(self, uri, credentials):
        self.spark_credentials = credentials
        self.spark_uri = uri
        self.spark_user_cookies = None
        self.spark_app_config = None
        print "Spark dashboard uri set to - " + self.spark_uri
        print "Spark dashboard credentials - " + str(self.spark_credentials)

    def __encode_and_prepare_datagen(self, filename):
        datagen, headers = multipart_encode({"file": open(filename, "rb")})
        datagen.params[0].name = 'jar'
        datagen.params[0].filetype = 'application/x-java-archive'
        self.__add_user_cookies_to_headers(headers)
        return datagen, headers

    def __add_user_cookies_to_headers(self, headers):
        if self.spark_user_cookies is not None:
            headers['Cookie'] = self.spark_user_cookies

    def __create_user_headers_with_cookies(self):
        headers = {}
        self.__add_user_cookies_to_headers(headers)
        return headers

    def __submit_app_jar(self, filename):
        # datagen, headers = self.__encode_and_prepare_datagen(filename)

        # Create the Request object
        # request_url = self.spark_uri + SparkApiConfig.call_submit
        # for the time being it is a local app. will be submitted to spark master in a cluster

        cmdline = "/opt/spark/bin/spark-submit --class org.oisp.RuleEngineBuild --master local[2] " + \
                  "/app/" + os.path.basename(filename) + \
                  " --runner=SparkRunner --JSONConfig='" + \
                  self.spark_app_config + \
                  "' --pipelineName=full";

        print json.dumps(self.spark_app_config);
        print ("Executing: ", cmdline);
        subprocess.call(cmdline, shell=True, stdout=subprocess.PIPE);
        response = "200 OK";
        return response;

    def __find_active_app_id_by_name(self, name):
        request = Request(self.spark_uri + SparkApiConfig.call_applist,
                          headers=self.__create_user_headers_with_cookies())

        json = util.call_api(request)

        for app in json['appMasters']:
            if app['appName'] == name and app['status'] == 'active':
                return app['appId']

    def __kill_app(self, app_id):
        request = Request(self.spark_uri + SparkApiConfig.call_appmaster + "/" + str(app_id),
                          headers=self.__create_user_headers_with_cookies())
        request.get_method = lambda: 'DELETE'
        return util.call_api(request)

    def __get_spark_user_cookies(self):
        request_url = self.spark_uri + SparkApiConfig.call_login
        body = self.spark_credentials
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        request = Request(url=request_url, data=urllib.urlencode(body), headers=headers)
        sock = urllib2.urlopen(request)
        cookies = sock.info()['Set-Cookie']
        sock.read()
        sock.close()
        self.spark_user_cookies = self.__parse_spark_user_cookies(cookies)

    def submit_app(self, filename, app_name, spark_app_config=None, force=False):
        print "Spark rule engine config"
        print str(spark_app_config)

        self.spark_app_config = util.json_dict_to_string(spark_app_config).replace(" ", "")

        response = self.__submit_app_jar(filename=filename)
        
        print 'Spark response: ' + response;
        #print response.text

    def __encode_spark_app_config(self, spark_app_config):
        return urllib.quote(util.json_dict_to_string(spark_app_config).replace(" ", ""))

    def __parse_spark_user_cookies(self, cookies):
        return cookies.split(';')[0] + '; username=' + self.spark_credentials['username']
