#!/bin/python
# -*- coding:UTF-8 -*-

import sys
import requests
import json

reload(sys)
sys.setdefaultencoding('utf-8')

url = 'http://192.168.204.128/'

param_list = {
    'values': json.dumps()
}

def request():
    requests.packages.urllib3.disable_warnings()
    result = requests.get(url, params=param_list)
    return result.content

if __name__ == '__main__':
    print request()
