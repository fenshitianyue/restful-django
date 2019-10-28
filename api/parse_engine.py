# -*- coding:UTF-8 -*-

# from django.http import HttpResponse
# from django.http import render_to_response

import api.models
import django.db.models
from django.core.serializers import serialize
from itertools import chain
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
    # if select.get('filter') is not None:
    #     result = table1.objects.filter(**select['filter'])
    # 处理 aggregation
    if select.get('aggregation') is not None:
        agg_dict = dict()
        for it in select['aggregation']:
            field = it[:it.find('_')]
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

        result = table1.objects.values(*select['group_by']).filter(**select['filter']). \
                 annotate(**agg_dict).order_by(*select['order_by'])
        #####################################
        table2 = getattr(api.models, 'Test2')
        fields = ['field1', 'field2']
        filters = {'field1': 'test1', 'field2__gt': 5, 'field3__contains': 'hello'}
        result2 = table2.objects.values(*fields).filter(**filters)
        result_all = chain(result, result2)
        #####################################

        print '----------------------------------------------'
        print result.query
        print '----------------------------------------------'
        print result2.query
        print '----------------------------------------------'
        for it in result_all:
            print it
        # print result_all
        print '----------------------------------------------'

    if result.exists():
        pass
        # serialize_data = serialize('json', result)
        # print serialize_data
        # print result[0].field1
        # print result[0].field3__count
        # for it in result:
        #     print it.field3__count
        #     print it.field4__sum
        #     print it.field4__avg
        #     print it.field5__count_distinct
    else:
        print "not found"
    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
    return result
