#!/usr/bin/python

import re
import time
import requests
import urllib
import argparse
from pprint import pprint

import json
import os
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

DEBUG = int(os.environ.get('DEBUG', '0'))


class NameNodeCollector(object):
    # The build statuses we want to export about.
    statuses = {
        "up": "node status. 1:up, 0:down",
        "MissingBlocks": "MissingBlocks",
        "CapacityTotal": "CapacityTotal",
        # "CapacityTotalGB": "CapacityTotalGB",
        "CapacityUsed": "CapacityUsed",
        # "CapacityUsedGB": "CapacityUsedGB",
        "CapacityRemaining": "CapacityRemaining",
        # "CapacityRemainingGB": "CapacityRemainingGB",
        "CapacityUsedNonDFS": "CapacityUsedNonDFS",
        "TotalLoad": "TotalLoad",
        "BlocksTotal": "BlocksTotal",
        "FilesTotal": "FilesTotal",
        "PendingReplicationBlocks": "PendingReplicationBlocks",
        "UnderReplicatedBlocks": "UnderReplicatedBlocks",
        "CorruptBlocks": "CorruptBlocks",
        "ScheduledReplicationBlocks": "ScheduledReplicationBlocks",
        "PendingDeletionBlocks": "PendingDeletionBlocks",
        "ExcessBlocks": "ExcessBlocks",
        "PostponedMisreplicatedBlocks": "PostponedMisreplicatedBlocks",
        "PendingDataNodeMessageCount": "PendingDataNodeMessageCount",
        "BlockCapacity": "BlockCapacity",
        "StaleDataNodes": "StaleDataNodes",
        "NumLiveDataNodes": "NumLiveDataNodes",
        "NumDeadDataNodes": "NumDeadDataNodes",
    }

    datanode_statuses = {
        "up": "node status. 1:up, 0:down",
        "blockPoolUsed": "blockPoolUsed",
        "blockPoolUsedPercent": "blockPoolUsedPercent",
        "capacity": "capacity, total space",
        "lastContact": "lastContact",
        "nonDfsUsedSpace": "nonDfsUsedSpace",
        "numBlocks": "numBlocks",
        "remaining": "remaining space",
        "used": "used space",
        # "usedSpace": "usedSpace", # same as used
    }

    quota_statuses = {
        "TotalFiles": "TotalFiles",
        "RemainFiles": "RemainFiles",
        "TotalSpace": "TotalSpace",
        "RemainSpace": "RemainSpace",
    }

    def __init__(self, target, cluster):
        self._cluster = cluster
        self._targets = target.rstrip("/").split(';')
        self._prefix = 'hadoop_namenode_'
        self._datanode_prefix = 'hadoop_datanode_node_'
        self._quota_prefix = 'hadoop_quota_'

    def collect(self):
        self._setup_empty_prometheus_metrics()
        # Request data from namenode jmx API
        for url in self._targets:
            self._target = url
            beans = self._request_data()
            self._get_metrics(beans)


        self._load_quota()

        for status in self.statuses:
            yield self._prometheus_metrics[status]

        for status in self.datanode_statuses:
            yield self._prometheus_datanode_metrics[status]

        for status in self.quota_statuses:
            yield self._prometheus_quota_metrics[status]

    def _request_data(self):
        # Request exactly the information we need from namenode
        # url = '{0}/jmx'.format(self._target)
        url = self._target

        def parsejobs(myurl):
            response = requests.get(myurl) #, params=params, auth=(self._user, self._password))
            if response.status_code != requests.codes.ok:
                return[]
            result = response.json()
            if DEBUG:
                pprint(result)

            return result['beans']

        return parsejobs(url)

    def _setup_empty_prometheus_metrics(self):
        # The metrics we want to export.
        self._prometheus_metrics = {}
        self._prometheus_datanode_metrics = {}
        self._prometheus_quota_metrics = {}
        for status in self.statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_metrics[status] = GaugeMetricFamily(self._prefix + snake_case,
                                      self.statuses[status], labels=["cluster", "nn_host", "nn_port"])

        for status in self.datanode_statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_datanode_metrics[status] = GaugeMetricFamily(self._datanode_prefix + snake_case,
                        self.datanode_statuses[status], labels=["cluster", "nn_host", "nn_port", "host", "xferaddr"])

        for status in self.quota_statuses:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            self._prometheus_quota_metrics[status] = GaugeMetricFamily(self._quota_prefix + snake_case,
                        self.quota_statuses[status], labels=["cluster", "dir"])

    def _get_metrics(self, beans):
        nn_host, nn_port = split_host_port(self._target)
        status = "up"
        self._prometheus_metrics[status].add_metric([self._cluster, nn_host, nn_port], 0 if beans == [] else 1)

        for bean in beans:
            if bean['name'] == "Hadoop:service=NameNode,name=FSNamesystemState":
                for status in self.statuses:
                    if bean.has_key(status):
                        self._prometheus_metrics[status].add_metric([self._cluster, nn_host, nn_port], bean[status])
            if bean['name'] == "Hadoop:service=NameNode,name=FSNamesystem":
                for status in self.statuses:
                    if bean.has_key(status):
                        self._prometheus_metrics[status].add_metric([self._cluster, nn_host, nn_port], bean[status])
            elif bean['name'] == "Hadoop:service=NameNode,name=NameNodeInfo":
                liveNodes = json.loads(bean['LiveNodes'])
                deadNodes = json.loads(bean['DeadNodes'])
                for host in liveNodes:
                    node = liveNodes[host]
                    node['up'] = 1
                    for status in self.datanode_statuses:
                        self._prometheus_datanode_metrics[status].add_metric(
                            [self._cluster, nn_host, nn_port, host, node['xferaddr'] ], node[status])
                for host in deadNodes:
                    node = deadNodes[host]
                    node['up'] = 0
                    for status in self.datanode_statuses:
                        if node.has_key(status):
                            self._prometheus_datanode_metrics[status].add_metric(
                                [self._cluster, nn_host, nn_port, host, node['xferaddr'] ], node[status])

    def _load_quota(self):
        quota_file = 'result'
        quota_dict = {}
        with open(quota_file, 'r') as f:
            for line in f.readlines():
                tags = line.strip().split()
                path = tags[len(tags) - 1]
                quota_dict["TotalFiles"] = str2float(tags[0])
                quota_dict["RemainFiles"] = str2float(tags[1])
                quota_dict["TotalSpace"] = str2float(tags[2])
                quota_dict["RemainSpace"] = str2float(tags[3])
                for status in self.quota_statuses:
                    self._prometheus_quota_metrics[self.quota_statuses[status]].add_metric(
                        [self._cluster, path], quota_dict[status])

def split_host_port(url):
    protocol, s1 = urllib.splittype(url)
    host, s2=  urllib.splithost(s1)
    return urllib.splitport(host)

def str2float(value):
    try:
        return float(value)
    except:
        return -1

def parse_args():
    parser = argparse.ArgumentParser(
        description='namenode exporter args namenode address and port'
    )
    parser.add_argument(
        '-url', '--namenode.jmx.url',
        metavar='url',
        dest='url',
        required=False,
        help='Hadoop NameNode JMX URL. (default "http://localhost:50070/jmx")',
        default='http://localhost:50070/jmx'
    )
    parser.add_argument(
        '--telemetry-path',
        metavar='telemetry_path',
        required=False,
        help='Path under which to expose metrics. (default "/metrics")',
        default='/metrics'
    )
    parser.add_argument(
        '-p', '--port',
        metavar='port',
        required=False,
        type=int,
        help='Listen to this port. (default ":9088")',
        default=int(os.environ.get('VIRTUAL_PORT', '9088'))
    )
    parser.add_argument(
        '--cluster',
        metavar='cluster',
        required=True,
        help='label for cluster'
    )
    return parser.parse_args()


def main():
    try:
        args = parse_args()
        port = int(args.port)
        REGISTRY.register(NameNodeCollector(args.url, args.cluster))

        start_http_server(port)
        print "Polling %s. Serving at port: %s" % (args.url, port)
        while True:
            os.system('bash quota_count.sh')
            time.sleep(3600)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()
