# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
from django.shortcuts import render
from django.http import HttpResponse
from . import parse_engine
from . import fake_data
# Create your views here.

def json_query(request):
    query = request.GET.get('values', None)
    if query is not None:
        query = json.loads(query)
    result = parse_engine.from_json_get_result(query)
    response = 'syntax correct~'
    return HttpResponse('<p>' + response + '</p>')

def insert_data(request):
    data = request.body
    if data is not None:
        data = json.loads(data)
    is_ok = fake_data.to_test1(data)
    if is_ok:
        return HttpResponse('<p>' + 'add a piece of data to |test1|' + '</p>')
    else:
        return HttpResponse('<p>' + 'add faild!' + '</p>')
