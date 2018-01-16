# -*- encoding: utf-8 -*-
import os
import sys
import time
import csv
import argparse
import datetime
import random
import string
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dateutil.relativedelta import relativedelta
import hashlib


INDEX_DATE_FORMAT = '%Y-%m-%d'


def set_data(input_file, tz, index_prefix):
    with open(input_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if not all(row.values()):
                sys.stdout.write('Could not import record "{0}" of file "{1}".\n'.format(row, input_file))
                continue

            index = hashlib.md5(''.join(row.values())).encode('utf-8')).hexdigest()
            now = system_tz.localize(datetime.datetime.now())
            yield {
                "_index": '{0}-{1}_{2}'.format(
                    index_prefix,
                    now.replace(day=1).strftime(INDEX_DATE_FORMAT),
                    (now.replace(day=1) + relativedelta(months=1)).strftime(INDEX_DATE_FORMAT)),
                "_type": 'data',
                '_id': index,
                "_source": row
            }


def parse_arguments():
    parser = argparse.ArgumentParser(description='Import Data into Elasticsearch')
    parser.add_argument('-ip', '--elastic-ip', metavar='ip', type=str, help='Elastic Search IP Address')
    parser.add_argument('-port', '--elastic-port', metavar='port', type=int, help='Elastic Search Listen Port')
    parser.add_argument('-d', '--source-directory', metavar='directory', type=str, help='Source Directory to Read Files from')
    parser.add_argument('-i', '--index-prefix', metavar='index', type=str, help='Prefix of Indices to be Inseting Iito ElasticSearch')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    es = Elasticsearch(['http://{0}:{1}'.format(args.elastic_ip, args.elastic_port)])
    source_dir = os.path.abspath(args.source_directory)
    sys.stdout.write('Reading files from root directory "{0}".\n'.format(source_dir))
    dirs = [os.path.join(source_dir, item) for item in os.listdir(source_dir) if os.path.isdir(os.path.join(source_dir, item))]
    system_tz = tzlocal.get_localzone()

    sys.stdout.write('Started importing at: {0}\n'.format(system_tz.localize(datetime.datetime.now()).isoformat()))
    start_time = time.time()
    for d in dirs:
        files = [os.path.join(d, item) for item in os.listdir(d)
                 if os.path.isfile(os.path.join(d, item)) and os.path.splitext(item)[1] == '.csv']
        for f in files:
            success, _ = bulk(es, set_data(f, system_tz, args.index_prefix.strip()))

    end_time = time.time()
    sys.stdout.write('Finished importing at: {0}\n'.format(system_tz.localize(datetime.datetime.now()).isoformat()))
    count = es.search(index="{0}*".format('{0}*'.format(args.index_prefix)), doc_type='data', body='', size=0)['hits']['total']
    sys.stdout.write('About {0} record(s) has been imported into '
                     'Elastic Search database in {1} second(s).\n'.format(count, end_time - start_time))
