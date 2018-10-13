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
from api_config import FlinkApiConfig
import util
import requests
import os
import subprocess
import json
import time


class FlinkApi:
    def __init__(self, uri):
        #self.flink_credentials = credentials
        self.flink_uri = uri
        self.flink_user_cookies = None
        self.flink_app_config = None
        print "Flink dashboard uri set to - " + self.flink_uri
        #print "Flink dashboard credentials - " + str(self.flink_credentials)

    def __encode_and_prepare_datagen(self, filename):
        datagen, headers = multipart_encode({"file": open(filename, "rb")})
        datagen.params[0].name = 'jar'
        datagen.params[0].filetype = 'application/x-java-archive'
        self.__add_user_cookies_to_headers(headers)
        return datagen, headers

    def __add_user_cookies_to_headers(self, headers):
        if self.flink_user_cookies is not None:
            headers['Cookie'] = self.flink_user_cookies

    def __create_user_headers_with_cookies(self):
        headers = {}
        self.__add_user_cookies_to_headers(headers)
        return headers

    def __submit_app_jar(self, filename):
        # datagen, headers = self.__encode_and_prepare_datagen(filename)

        # Create the Request object
        # request_url = self.flink_uri + FlinkApiConfig.call_submit
        # for the time being it is a local app. will be submitted to flink master in a cluster
        # Submit 3 different pipelines: Heartbeat, Rules-update, observations
        # ONLY observations pipeline can be scaled!
        
        cmdline_heartbeat = "java -cp ../target/" + filename + " org.oisp.RuleEngineBuild --flinkMaster=" + self.flink_uri + \
                  " --runner=FlinkRunner --JSONConfig=config.json" + \
                  " --pipelineName=heartbeat --streaming=true --filesToStage=../target/" + filename;
        cmdline_rupdate = "java -cp ../target/" + filename + " org.oisp.RuleEngineBuild --flinkMaster=" + self.flink_uri + \
                  " --runner=FlinkRunner --JSONConfig=config.json" + \
                  " --pipelineName=rules-update --streaming=true --filesToStage=../target/" + filename;
        cmdline_observations = "java -cp ../target/" + filename + " org.oisp.RuleEngineBuild --flinkMaster=" + self.flink_uri + \
                  " --runner=FlinkRunner --JSONConfig=config.json" + \
                  " --pipelineName=observations --streaming=true --filesToStage=../target/" + filename;

        print json.dumps(self.flink_app_config);
        f = open("config.json", "w");
        f.write(self.flink_app_config);
        f.close()
        print ("Executing: ", cmdline_rupdate);
        subprocess.Popen(args=cmdline_rupdate.split(" "));

        print ("Executing: ", cmdline_observations);
        subprocess.Popen(cmdline_observations.split(" "));

        print ("Executing: ", cmdline_observations);
        subprocess.Popen(cmdline_observations.split(" "));

        print ("Executing: ", cmdline_heartbeat);
        subprocess.Popen(cmdline_heartbeat.split(" "));

        time.sleep(5);
        
        response = "200 OK";
        return response;


    def submit_app(self, filename, app_name, flink_app_config=None, force=False):
        print "Flink rule engine config"
        print str(flink_app_config)

        self.flink_app_config = util.json_dict_to_string(flink_app_config).replace(" ", "")

        response = self.__submit_app_jar(filename=filename)
        
        print 'Flink response: ' + response;
        #print response.text

    def __encode_flink_app_config(self, flink_app_config):
        return urllib.quote(util.json_dict_to_string(flink_app_config).replace(" ", ""))

 #   def __parse_flink_user_cookies(self, cookies):
 #       return cookies.split(';')[0] + '; username=' + self.flink_credentials['username']
