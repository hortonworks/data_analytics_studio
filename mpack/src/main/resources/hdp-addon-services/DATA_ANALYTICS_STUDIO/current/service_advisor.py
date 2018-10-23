#!/usr/bin/env ambari-python-wrap
"""
  HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES

  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
"""

import imp
import os
import socket
import traceback

from resource_management.libraries.functions.data_structure_utils import get_from_dict

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))))
STACKS_DIR = os.path.join(SCRIPT_DIR, 'stacks')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"

class HDP30DATAANALYTICSSTUDIOServiceAdvisor(service_advisor.ServiceAdvisor):

  def __init__(self, *args, **kwargs):
    self.as_super = super(HDP30DATAANALYTICSSTUDIOServiceAdvisor, self)
    self.as_super.__init__(*args, **kwargs)
    self.initialize_logger("HDP30DATAANALYTICSSTUDIOServiceAdvisor")

  def getHostForComponent(self, component, hostsList):
    if len(hostsList) == 0:
      return None

    componentName = self.getComponentName(component)
    if componentName == "DATA_ANALYTICS_STUDIO_WEBAPP":
      self.logger.info("Checking for postgres on host: " + socket.getfqdn());
      # The local host with ambari is likely to have postgresql installed on it, do not put Data Analytics Studios DB here
      result = os.system("which psql > /dev/null 2>&1")
      if result == 0:
        self.logger.info("Ambari host ({0}) has postgresql db, looking for other host for DATA_ANALYTICS_STUDIO_WEBAPP"
                   .format(socket.getfqdn()))
        for host in hostsList:
          if host != socket.getfqdn():
            self.logger.info("DATA_ANALYTICS_STUDIO_WEBAPP was put into " + host)
            return host
    
    return super(HDP30DATAANALYTICSSTUDIOServiceAdvisor, self).getHostForComponent(component, hostsList)

  def colocateService(self, hostsComponentsMap, serviceComponents):
    pass
  
  def getServiceComponentLayoutValidations(self, services, hosts):
    items = []
    
    componentsListList = [service["components"] for service in services["services"]]
    componentsList = [item["StackServiceComponents"] for sublist in componentsListList for item in sublist]
    dasPostgresqlServerHost = self.getHosts(componentsList, "DATA_ANALYTICS_STUDIO_WEBAPP")[0]
    
    result = os.system("which psql > /dev/null 2>&1")
    if result == 0 and socket.getfqdn() == dasPostgresqlServerHost:
      items.append( { "type": 'host-component',
                      "level": 'WARN',
                      "message": "Data Analytics Studio PostgreSQL Server is put on the same host as Ambari, where it is running it's own PostgreSQL server. The two may collide.",
                      "component-name": 'DATA_ANALYTICS_STUDIO_WEBAPP',
                      "host": dasPostgresqlServerHost } )
    
    return items

  def appendToProperty(self, configurations, services, configType, propertyName, propertyValues, setAllValue):
    currentValue = get_from_dict(services, ("configurations", configType, "properties", propertyName), default_value="")
    if currentValue != setAllValue:
      putProperty = self.putProperty(configurations, configType, services)
      diff = set(propertyValues) - set(currentValue.split(','))
      if len(diff) > 0:
        propertyValue = ','.join(diff)
        newValue = currentValue + ',' + propertyValue if len(currentValue) > 0 else propertyValue
        self.logger.info("Setting {0} to {1}".format(propertyName, newValue))
        putProperty(propertyName, newValue)

  def getServiceConfigurationRecommendations(self, configurations, clusterSummary, services, hosts):
    servicesList = set([service['StackServices']['service_name'] for service in services['services']])
    self.logger.info("Conf recommendations for DAS")
    putDasSecurityProperty = self.putProperty(configurations, 'data_analytics_studio-security-site', services)
    das_user = get_from_dict(services, ("configurations", "hive-env", "properties", "hive_user"), default_value=None)
    putDasSecurityProperty('admin_users', das_user)
    if 'KNOX' in servicesList:
      self.logger.info("configuring knox ...")
      knox_port = '8443'
      knox_hosts = self.getComponentHostNames(services, "KNOX", "KNOX_GATEWAY")
      if len(knox_hosts) > 0:
        knox_hosts.sort()
        knox_host = knox_hosts[0]
        knox_port = get_from_dict(services, ("configurations", "gateway-site", "properties", "gateway.port"), default_value=knox_port)
        putDasSecurityProperty('knox_sso_enabled', 'true')
        putDasSecurityProperty('knox_sso_url', 'https://{0}:{1}/gateway/knoxsso/api/v1/websso'.format(knox_host, knox_port))
        self.logger.info("knox host: {0}, knox port: {1}".format(knox_host, knox_port))
    if 'HDFS' in servicesList and 'core-site' in services['configurations']:
      self.logger.info("setting up proxy hosts")
      das_hosts = self.getComponentHostNames(services, 'DATA_ANALYTICS_STUDIO', 'DATA_ANALYTICS_STUDIO_WEBAPP')
      das_hosts = das_hosts + self.getComponentHostNames(services, 'DATA_ANALYTICS_STUDIO', 'DATA_ANALYTICS_STUDIO_EVENT_PROCESSOR')
      propertyName = 'hadoop.proxyuser.{0}.hosts'.format(das_user)
      self.appendToProperty(configurations, services, 'core-site', propertyName, das_hosts, '*')

  def getServiceConfigurationRecommendationsForSSO(self, configurations, clusterData, services, hosts):
    self.logger.info("setting up conf for sso")
    ambari_configuration = self.get_ambari_configuration(services)
    ambari_sso_details = ambari_configuration.get_ambari_sso_details() if ambari_configuration else None
    if ambari_sso_details and ambari_sso_details.is_managing_services():
      putProperty = self.putProperty(configurations, "data_analytics_studio-security-site", services)
      if ambari_sso_details.should_enable_sso('DATA_ANALYTICS_STUDIO'):
        self.logger.info("enabling sso for das")
        putProperty('knox_sso_enabled', 'true')
        putProperty('knox_sso_url', ambari_sso_details.get_sso_provider_url())
        putProperty('knox_publickey', ambari_sso_details.get_sso_provider_certificate(False, True))
      elif ambari_sso_details.should_disable_sso('DATA_ANALYTICS_STUDIO'):
        self.logger.info("disabling sso for das")
        putProperty('knox_sso_enabled', 'false')
