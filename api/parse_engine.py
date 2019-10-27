# -*- coding:UTF-8 -*-

# from django.http import HttpResponse
# from django.http import render_to_response

import api.models
import django.db.models
from django.core.serializers import serialize
import json
# from django.db.models import Sum, Count, Avg
# import sys
# reload(sys)
# sys.getdefaultencoding('utf-8')

def get_model(model_name):
    return getattr(api.models, model_name)

# version1:先测一下单表解析
def from_json_get_result(query):
    select = dict()
    if query.get('select'):
        select = query['select']
    table1 = get_model(select['from'])
    # 处理 filter
    if select.get('filter') is not None:
        result = table1.objects.filter(**select['filter'])
    # 处理 aggregation
    if select.get('aggregation') is not None:
        agg_dict = dict()
        for it in select['aggregation']:
            field = it[:it.find('_')]
            print it
            print field
            if it.find('count') != -1:
                if it.find('distinct') != -1:
                    agg_dict.update({it: getattr(django.db.models, 'Count')(field, distinct=True)})
                else:
                    agg_dict.update({it: getattr(django.db.models, 'Count')(field)})
            if it.find('sum') != -1:
                if it.find('distinct') != -1:
                    agg_dict.update({it: getattr(django.db.models, 'Sum')(field, distinct=True)})
                else:
                    agg_dict.update({it: getattr(django.db.models, 'Sum')(field)})
            if it.find('avg') != -1:
                if it.find('distinct') != -1:
                    agg_dict.update({it: getattr(django.db.models, 'Avg')(field, distinct=True)})
                else:
                    agg_dict.update({it: getattr(django.db.models, 'Avg')(field)})
        result |= table1.objects.annotate(**agg_dict)

    if result.exists():
        # serialize_data = serialize('json', result)
        # print serialize_data
        print result[0].field1
        print result[0].field2
        for it in result:
            print it.field3__count
            print it.field4__sum
            print it.field4__avg
            print it.field5__count_distinct
    else:
        print "not found"
    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
    return result
