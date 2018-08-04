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

import vcap


class CloudfoundryBridge(object):
    def __init__(self):
        self._vcap_services = vcap.VcapServices()
        self._vcap_application = vcap.VcapApplication()

    @property
    def spark_dashboard_url(self):
        return "http://" + self._vcap_services.spark_url

    @property
    def spark_credentials(self):
        if not self.__is_spark_auth_enabled():
            return None
        return {
            'username': self._vcap_services.spark_username,
            'password': self._vcap_services.spark_password
        }

    def __is_spark_auth_enabled(self):
        return hasattr(self._vcap_services, 'spark_username')

    def build_config(self, local=False):
        kafka_zookeeper_quorum = self._vcap_services.kafka_zookeeper_quorum

        if not local:
            kafka_zookeeper_quorum += "/kafka"

        config = {
            "zookeeper_hbase_quorum": self._vcap_services.zookeeper_hbase_quorum,
            "hbase_table_prefix": self._vcap_application.space_name,
            "token": self._vcap_services.token,
            "dashboard_url": self._vcap_services.dashboard_url,
            "kafka_servers": self._vcap_services.kafka_servers,
            "kafka_observations_topic": self._vcap_services.observations_topic_name,
            "kafka_rule_engine_topic": self._vcap_services.rule_engine_topic_name,
            "kafka_heartbeat_topic": self._vcap_services.heartbeat_topic_name,
            "kafka_heartbeat_interval": self._vcap_services.heartbeat_interval,
            "kafka_zookeeper_quorum": kafka_zookeeper_quorum,
            "application_name": "rule_engine_" + self._vcap_services.dashboard_url_normalized_for_spark,
            "dashboard_strict_ssl": self._vcap_services.dashboard_strict_ssl,
            "hadoop_security_authentication": self._vcap_services.hadoop_security_authentication
        }

        if self._vcap_services.is_kerberos_enabled():
            config["krb_master_principal"] = self._vcap_services.krb_master_principal
            config["krb_regionserver_principal"] = self._vcap_services.krb_regionserver_principal
            config["krb_kdc"] = self._vcap_services.krb_kdc
            config["krb_password"] = self._vcap_services.krb_password
            config["krb_user"] = self._vcap_services.krb_user
            config["krb_realm"] = self._vcap_services.krb_realm


        return config
