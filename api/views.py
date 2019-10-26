# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
from django.shortcuts import render
from django.http import HttpResponse
from . import parse_engine
# Create your views here.

def json_query(request):
    query = request.GET.get('values', None)
    if query is not None:
        query = json.loads(query)
    result = parse_engine.from_json_get_result(query)
    response = 'syntax correct~'
    return HttpResponse('<p>' + response + '</p>')