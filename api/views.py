# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
from django.http import HttpResponse
from . import parse_engine
from . import fake_data
# Create your views here.

def pg_query(request):
    query = request.GET.get('values', None)
    if query is not None:
        query = json.loads(query)
    # token = request.META.get('token')
    parse_engine.pg_query(query)
    response = 'syntax correct~'
    return HttpResponse('<p>' + response + '</p>')

def insert_data(request):
    data = request.body
    if data is not None:
        data = json.loads(data)
    # is_ok = fake_data.to_test1(data)
    # is_ok = fake_data.to_test2(data)
    is_ok = fake_data.to_test3(data)
    if is_ok:
        return HttpResponse('<p>' + 'add a piece of data to |Test x|' + '</p>')
    else:
        return HttpResponse('<p>' + 'add faild!' + '</p>')

def es_query(request):
    query = request.GET.get('values', None)
    if query is not None:
        query = json.loads(query)
    parse_engine.es_query(query)
    response = 'syntax correct~'
    return HttpResponse('<p>' + response + '</p>')

