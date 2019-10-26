# -*- coding:UTF-8 -*-

# from django.http import HttpResponse
# from django.http import render_to_response

import api.models
from django.db.models import Sum, Count, Avg
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
    # if select.get('filter') is not None:
    #     result = table1.objects.filter(**select['filter'])
    # 处理 aggregation
    if select.get('aggregation') is not None:
        agg_dict = dict()
        agg_dict = dict({'field3_count' : getattr(result, 'Count')("field3")})
        # for it in select['aggregation']:
        #     agg_dict.update({it : it})
        result = table1.objects.annotate(**agg_dict)
        # result = table1.objects.annotate(select['aggregation'])
        # result |= table1.objects.values('field1').annotate(Count('field1')).distinct()
        # for agg_field in select['aggregation']:
        #     field = agg_field[:agg_field.find('_')]
        #     if agg_field.find('count') != -1:
        #         if agg_field.find('distinct') != -1:
        #             result |= table1.objects.annotate(Count(field)).distinct(field)
        #         else:
        #             result |= table1.objects.annotate(Count(field))
        #     if agg_field.find('sum') != -1:
        #         if agg_field.find('distinct') != -1:
        #             result |= table1.objects.annotate(Sum(field)).distinct(field)
        #         else:
        #             result |= table1.objects.annotate(Sum(field))
        #     if agg_field.find('avg') != -1:
        #         if agg_field.find('distinct') != -1:
        #             result |= table1.objects.annotate(Avg(field)).distinct(field)
        #         else:
        #             result |= table1.objects.annotate(Avg(field))

    for it in result:
        print it

    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
    return result
