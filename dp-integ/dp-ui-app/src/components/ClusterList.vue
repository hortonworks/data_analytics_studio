<!--
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
-->
<template>
  <div class="ds-dp-integ">
    <div class="header">
      <div class="header-breadcrumb">
        Data Analytics Studio
      </div>
    </div>
    <div class="das-dp-nav" id="das-dp-nav">
      <img src="../assets/img/icon-DAS-green-42.png" title="Data Analytics Studio" />
    </div>
    <div class="das-clusters-container">
      <div class="das-clusters">
        <p class="das-info">Using data analytcis studio you can execute and understand performance characterstics of your cluster.</p>
        <div class="hwx-title">
          Cluster Information
        </div>
        <div id="clusters-list">
          <div class="loader-container" v-if="loading">
            <div class="loader"></div>
          </div>
          <div class="no-cluster-info" v-else-if="!data.length">
              <h1>No clusters configured</h1>
          </div>
          <form class="panel cluster-details-form" v-for="item of data">
            <div class="gbackround color1">
              <div class="highlight">
              </div>
              <div class="textblock">
                  <label>Cluster Name:</label> {{ item.name }} <br />
                  <label>HDP version:</label> {{item.hdp_version }} <br />
                  <label>DAS version:</label> {{ item.das_version }}<br />
                  <label>Nodes:</label> {{ item.num_nodes }} <br />
              </div>
              <div class="textblock mini-meta-data">
                <div class="text" v-if="item.hdfs_total_storage">
                  {{ item.hdfs_used_percent}}% / {{ item.hdfs_total_storage }}
                </div>
                <div class="text" v-else>
                  Not Available
                </div>
                <div class="textitle">Current storage / capacity</div>
              </div>
              <div class="textblock mini-meta-data">
                <div class="text" v-if="item.yarn_memory_usage">{{ item.yarn_memory_usage }}%</div>
                <div class="text" v-else>Not Available</div>
                <div class="textitle">Yarn memory usage</div>
              </div>
              <div class="connectbutton">
                <a v-bind:href=item.das_url target="_blank">Connect</a>
              </div>
            </div>
          </form>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
  import axios from 'axios';
  import {DpAppNavigation} from 'dps-apps';

  export default {
  name: 'ClusterList',
  data () {
    return {
      loading : true,
      data : []
    }
  },
  mounted() {
    this.loading = true;

    axios.get('/api/clusters?plugin=das')
      .then((response) => {
        let clusters = response.data;
        var promises = [], promise;
        for (var i = 0; i < clusters.length; ++i) {
          var cluster = clusters[i];
          var cobj = new Object();

          cobj.name = cluster.name;
          cobj.hdp_version = cluster.version;
          cobj.num_nodes = cluster.properties.total_hosts;

          // Figure out how to get das_version and llap status.
          cobj.das_version = 'Not Available!';

          // There is id and dataplaneClusterId which one to use?
          let url1 = '/api/clusters/' + cluster.id + '/services?serviceName=DATA_ANALYTICS_STUDIO',
          url2 = '/api/clusters/' + cluster.id + '/health?dpClusterId=' + cluster.dataplaneClusterId,
          url3 = '/api/clusters/' + cluster.id + '/rmhealth',
          results = [];

          (function(cobj, cluster){
          promise = axios.get(url1)
          .then(function(data){
            results.push(data);
            return axios.get(url2)
          })
          .catch(function(error){
            results.push({});
            return axios.get(url2)
          })
          .then(function(data){
            results.push(data);
            return axios.get(url3)
          })
          .catch(function(error){
            results.push({});
            return axios.get(url3)
          })
          .then(function(data){
            results.push(data);
            return parseResults(results, cobj, cluster);
          })
          .catch(function(error){
            results.push({});
            return parseResults(results, cobj, cluster);
          });

          function getConfigurations(dasInfo, confName) {
            let properties = dasInfo.properties;
            for(let index in properties) {
              if(properties[index].type == confName) {
                return properties[index];
              }
            }
          }

          function extractVersion(dasConf) {
            try {
              let props = dasConf.split(/\r\n|\n|\r|↵/);
              for(let index in props) {
                if(props[index].indexOf("application.version") == 0) {
                  return props[index].split("=")[1];
                }
              }
            }catch(e){}
            return "Not Available!";
          }

          function parseResults(responses, cobj, cluster) {
              try {
                let dasInfo = responses[0].data[0];
                let hostname = dasInfo.hosts[0];
                let hostProps = getConfigurations(dasInfo, 'data_analytics_studio-webapp-properties').properties;
                let port = hostProps.data_analytics_studio_webapp_server_port;
                let scheme = hostProps.data_analytics_studio_webapp_server_protocol;
                cobj.das_url = '/api/clusters/' + cluster.dataplaneClusterId + '/redirect?to=' + scheme + '://' + hostname + ':' + port;

                let dasConf = getConfigurations(dasInfo, 'data_analytics_studio-properties').properties.content;
                cobj.das_version = extractVersion(dasConf);

                let nnInfo = responses[1].data.nameNodeInfo;
                cobj.hdfs_used_percent = (nnInfo.CapacityUsed / nnInfo.CapacityTotal * 100).toFixed(2);

                var cap = nnInfo.CapacityTotal;
                var i = 0;
                var arr = ['B', 'K', 'M', 'G', 'T', 'P'];
                while (cap > 1024 && i < arr.length - 1) { cap = cap / 1024; i++}
                cobj.hdfs_total_storage = cap.toFixed(2) + ' ' + arr[i];

                let rmInfo = responses[2].data.metrics.jvm;
                cobj.yarn_memory_usage = (rmInfo.HeapMemoryUsed / rmInfo.HeapMemoryMax * 100).toFixed(2);
              } catch(e) {}
              finally {
                return cobj;
              }
          }
          })(cobj, cluster);
          promises[i] = promise.then(function(data){
            return data;
          });
        }
        return Promise.all(promises);
      })
      .then((values) => {
        this.loading = false;
        this.data = values;
      });


  },
  updated() {
    DpAppNavigation.init({
        srcElement: document.getElementById('das-dp-nav'),
        assetPrefix: './img/'
    });
  }
}


</script>



<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h1, h2 {
  font-weight: normal;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: white;
}
.no-cluster-info {
  left: 40%;
  position: relative;
  top: 10px;
}
</style>
