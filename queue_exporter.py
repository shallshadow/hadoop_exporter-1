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


class YarnQueueCollector(object):
    # The build statuses we want to export about.
    queue_statues = {
        "allocatedContainers":"allocatedContainers",
        "clusterResources":"clusterResources",
        "demandResources":"demandResources",
        "fairResources":"fairResources",
        "numActiveApps":"numActiveApps",
        "numPendingApps":"numPendingApps",
        "reservedContainers":"reservedContainers",
        "steadyFairResources":"steadyFairResources",
        "usedResources":"usedResources",
    }

    def __init__(self, target, cluster):
        self._cluster = cluster
        self._targets = target.rstrip("/").split(';')
        self._prefix = 'yarn_queue_'

    def collect(self):
        self._setup_empty_prometheus_metrics()
        # Request data from namenode jmx API
        for url in self._targets:
            self._target = url
            self.rm_host, self.rm_port = split_host_port(self._target)
            beans = self._request_data()
            self._get_metrics(beans)


        for status in self.queue_statues:
            yield self._prometheus_metrics[status]

    def _request_data(self):
        # Request exactly the information we need from namenode
        url = '{0}/ws/v1/cluster/scheduler'.format(self._target)

        def parsejobs(myurl):
            response = requests.get(myurl) #, params=params, auth=(self._user, self._password))
            if response.status_code != requests.codes.ok:
                return[]
            result = response.json()
            if DEBUG:
                pprint(result)

            return result['scheduler']['schedulerInfo']['rootQueue']

        return parsejobs(url)

    def _setup_empty_prometheus_metrics(self):
        # The metrics we want to export.
        self._prometheus_metrics = {}
        for status in self.queue_statues:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', status).lower()
            if "Resources" in status:
                self._prometheus_metrics[status] = GaugeMetricFamily(
                    self._prefix + snake_case,
                    self.queue_statues[status],
                    labels=["cluster", "rm_host", "rm_port", "queue_name", "res_type"])
            else:
                    self._prometheus_metrics[status] = GaugeMetricFamily(self._prefix + snake_case,
                                      self.queue_statues[status], labels=["cluster", "rm_host", "rm_port", "queue_name"])


    def _get_metrics(self, beans):
        self._tree_list(beans)


    def _tree_list(self, root):
        if "childQueues" in root:
            self._set_metrics(root)
            self._tree_list(root["childQueues"])
        elif "queue" in root:
            self._tree_list(root["queue"])
        elif isinstance (root, list):
            for queue in root:
                self._tree_list(queue)
        else:
            self._set_metrics(root)

    def _set_metrics(self, root):
        queue_name = root["queueName"]
        for key in root:
            if key in self.queue_statues:
                if "Resources" in key:
                    resources_dict = root[key]
                    for res_key in resources_dict:
                        self._prometheus_metrics[key].add_metric([self._cluster, self.rm_host, self.rm_port, queue_name, res_key], resources_dict[res_key])
                else:
                    self._prometheus_metrics[key].add_metric([self._cluster, self.rm_host, self.rm_port, queue_name], root[key])

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
        description='yarn queue exporter args yarn address and port'
    )
    parser.add_argument(
        '-url', '--namenode.jmx.url',
        metavar='url',
        dest='url',
        required=False,
        help='Hadoop Yarn FairScheduler JMX URL. (default "http://localhost:8088")',
        default='http://localhost:8088'
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
        REGISTRY.register(YarnQueueCollector(args.url, args.cluster))
        #REGISTRY.register(ResourceManagerNodeCollector(args.url, args.cluster))

        start_http_server(port)
        print "Polling %s. Serving at port: %s" % (args.url, port)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()
