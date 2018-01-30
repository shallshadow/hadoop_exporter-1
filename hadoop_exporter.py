#!/usr/bin/python
import argparse
import os
import time

from namenode_exporter import NameNodeCollector
from resourcemanager_exporter import ResourceManagerNodeCollector
from resourcemanager_exporter import ResourceManagerCollector
from queue_exporter import YarnQueueCollector
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY

def parse_args():
    parser = argparse.ArgumentParser(
        description='hadoop exporter args namenode address and port and resourcemanager address and port'
    )

    parser.add_argument(
        '-nnurl', '--namenode.jmx.url',
        metavar='nnurl',
        dest='nnurl',
        required=False,
        help='Hadoop NameNode JMX URL. (default "http://localhost:50070/jmx") And you can use ";" to enable ha',
        default='http://localhost:50070/jmx'
    )

    parser.add_argument(
        '-rmurl', '--resourcemanager.url',
        metavar='rmurl',
        dest='rmurl',
        required=False,
        help='Hadoop ResourceManager URL. (default "http://localhost:8088") And you can use ";" to enable ha',
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

        REGISTRY.register(NameNodeCollector(args.nnurl, args.cluster))
        REGISTRY.register(ResourceManagerCollector(args.rmurl, args.cluster))
        REGISTRY.register(ResourceManagerNodeCollector(args.rmurl, args.cluster))
        REGISTRY.register(YarnQueueCollector(args.rmurl, args.cluster))

        port = int(args.port)
        start_http_server(port)
        print "Polling %s. Serving at port: %s" % (args.nnurl, port)
        print "Polling %s. Serving at port: %s" % (args.rmurl, port)
        while True:
            os.system('bash quota_count.sh')
            time.sleep(3600)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()
