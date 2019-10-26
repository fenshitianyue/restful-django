# -*- coding:UTF-8 -*-

# from django.http import HttpResponse
# from django.http import render_to_response

import api.models
from django.db.models import Sum, Count, Avg

import sys
reload(sys)
sys.getdefaultencoding('utf-8')

def get_model(model_name):
    return getattr(api.models, model_name)

def json_query(request):
    pass

