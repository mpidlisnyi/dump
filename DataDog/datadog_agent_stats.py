#!/usr/bin/python2
import requests
import json
import csv
from urllib import urlencode
from datetime import datetime
from boto3 import client

'''
DataDog does not provide any searching by agents version
This script creates ES datadog indices for reviewing agent version with Kibana
tested on ES 7
"datadog" ES alias must be created manually after first script run
'''

def get_parameter(name):
    return client('ssm', region_name='us-east-1').get_parameter(
        Name=name,
        WithDecryption=True
        )['Parameter']['Value']

url = 'https://app.datadoghq.com/reports/v2/overview'

params = {
    'api_key': get_parameter('/datadog/team/api_key'),
    'application_key': get_parameter('/datadog/team/app_key'),
    'window': '3h',
    'metrics': 'avg:system.cpu.idle,avg:aws.ec2.cpuutilization,avg:vsphere.cpu.usage,avg:azure.vm.processor_total_pct_user_time,avg:system.cpu.iowait,avg:system.load.norm.15',
    'with_meta': True,
    'with_mute_status': True,
    'with_tags': True,
#    'tags': 'main_aws_account',
}

headers = {
    'content-type': 'application/json',
    'Accept-Charset': 'UTF-8',
    }


def agent_versions(data, batch_size=50, host_url='http://localhost:9200'):
    index = 'datadog-{}'.format(datetime.now().strftime("%Y%m%d%H%M%S"))
    counter = 0
    buff = ''
    for hosts in data['rows']:
        meta = hosts['meta']
        if 'agent_version' in meta:
            counter += 1
            version = meta['agent_version'].split('.')

            major = version[0]
            minor = version[1]

            if 'Datadog' in hosts['tags_by_source']:
                dd_tags = hosts['tags_by_source']['Datadog']
            else:
                dd_tags = []

            if 'Amazon Web Services' in hosts['tags_by_source']:
                aws_tags = hosts['tags_by_source']['Amazon Web Services']
            else:
                aws_tags = []

            bulk_meta = json.dumps(
                    {
                        'index': {
                            '_index': index,
                            '_type': '_doc',
                            }
                        }
                )
            data = json.dumps(
                    {
                    'host': hosts['host_name'],
                    'major': major,
                    'minor': minor,
                    'agent_tags': dd_tags,
                    'aws_tags': aws_tags,
                    'type': 'agent-stats',
                    }
                )

            buff += '{}\n{}\n'.format(bulk_meta, data)

            if counter % batch_size == 0:
                response = requests.post(
                    '{}/_bulk'.format(host_url),
                    data=buff,
                    headers=headers
                    )
                counter = 0
                buff = ''
    if buff:
        requests.post(
            '{}/_bulk'.format(host_url),
            data=buff,
            headers=headers
            )

    return index


def alias(alias_name, new_index, old_index=None, host_url='http://localhost:9200'):
    data = {
        'actions' : [
            {
                'add': {
                    'index': new_index,
                    'alias': alias_name,
                    },
                },
        ]
    }

    if old_index:
        data['actions'].append({ 'remove_index': {'index': old_index} })

    response = requests.post(
        '{}/_aliases'.format(host_url),
        data=json.dumps(data),
        headers=headers
        )

    if response.status_code != 200:
        raise(
            Exception('API returns non 200 code {}'.format(
                response.status_code)
                )
            )

def get_alias_index(alias_name, host_url='http://localhost:9200'):
    response = requests.get(
        '{}/_alias/{}'.format(host_url, alias_name),
        headers=headers
        )
    if response.status_code == 200:
        return response.json().keys()[0]
    else:
        raise(
            Exception('API returns non 200 code {}'.format(
                response.status_code)
                )
            )

def main():
    r = requests.get(url, params=urlencode(params), headers=headers)

    if r.status_code == 200:
        new_index = agent_versions(r.json())
        # you must have aliase "datadog"
        old_index = get_alias_index('datadog')
        alias('datadog', new_index, old_index)
    else:
        print 'API returns non 200 code {}'.format(r.status_code)
        exit(1)

if __name__ == '__main__':
    main()

