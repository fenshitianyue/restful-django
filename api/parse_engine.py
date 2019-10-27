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
    result = table1.objects.all()
    # 处理 filter
    if select.get('filter') is not None:
        result = table1.objects.filter(**select['filter'])
    # 处理 aggregation
    if select.get('aggregation') is not None:
        agg_dict = {
            'field3__count': getattr(django.db.models, 'Count')('field3'),
            'field4__sum': getattr(django.db.models, 'Sum')('field4'),
            'field4__avg': getattr(django.db.models, 'Avg')('field4'),
            'field5__count_distinct': getattr(django.db.models, 'Count')('field5', distinct=True)
        }
        result = table1.objects.annotate(**agg_dict)
        # for it in select['aggregation']:
        #     pass
        # result = table1.objects.annotate(Count('field3'), Sum('field4'), Avg('field4'), Count('field5', distinct=True))
        # for agg_field in select['aggregation']:
        #     field = agg_field[:agg_field.find('_')]
        #     if agg_field.find('count') != -1:
        #         if agg_field.find('distinct') != -1:
        #             # values用于去重，order_by用于定义别名
        #             result |= table1.objects.annotate(Count(field)).distinct(field).order_by(field)
        #         else:
        #             result |= table1.objects.annotate(Count(field))
        #     if agg_field.find('sum') != -1:
        #         if agg_field.find('distinct') != -1:
        #             result |= table1.objects.values(field).annotate(Sum(field)).order_by(field)
        #         else:
        #             result |= table1.objects.annotate(Sum(field))
        #     if agg_field.find('avg') != -1:
        #         if agg_field.find('distinct') != -1:
        #             result |= table1.objects.values(field).annotate(Avg(field)).order_by(field)
        #         else:
        #             result |= table1.objects.annotate(Avg(field))

    if result.exists():
        serialize_data = serialize('json', result)
        print serialize_data
    else:
        print "not found"
    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
    return result
