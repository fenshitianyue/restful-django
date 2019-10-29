# -*- coding:UTF-8 -*-

import api.models
import django.db.models
import pandas as pd

def get_model(model_name):
    return getattr(api.models, model_name)

def get_attribute(attribute_name):
    return getattr(django.db.models, attribute_name)

def from_json_get_result(query):
    select = dict()
    result_set = list()
    # 先拿出select字段
    if query.get('select'):
        select = query['select']
    table1 = get_model(select['from'])
    if select.get('filter') is not None:
        had_filter = True
    if select.get('aggregation') is not None:
        had_agg = True
        agg_dict = dict()
        for it in select['aggregation']:
            field = it[:it.find('_')]
            if it.find('count') != -1:
                if it.find('distinct') != -1:
                    agg_dict.update({it: get_attribute('Count')(field, distinct=True)})
                else:
                    agg_dict.update({it: get_attribute('Count')(field)})
            if it.find('sum') != -1:
                if it.find('distinct') != -1:
                    agg_dict.update({it: get_attribute('Sum')(field, distinct=True)})
                else:
                    agg_dict.update({it: get_attribute('Sum')(field)})
            if it.find('avg') != -1:
                if it.find('distinct') != -1:
                    agg_dict.update({it: get_attribute('Avg')(field, distinct=True)})
                else:
                    agg_dict.update({it: get_attribute('Avg')(field)})
    if select.get('limit') is not None:
        if select['limit'] < 10000:
            table1_limit = select['limit']
        else:
            table1_limit = 10000
    else:
        table1_limit = 10000
    print agg_dict
    if had_filter and had_agg:
        result = table1.objects.values(*select['group_by']).filter(**select['filter']).annotate(**agg_dict)
    elif not had_filter and had_agg:
        result = table1.objects.values(*select['group_by']).annotate(**agg_dict)
    elif had_filter and not had_agg:
        result = table1.objects.values(*select['group_by']).filter(**select['filter'])
    else:
        pass

    if select.get('order_by') is not None:
        result = result.objects.values(*select['order_by'])[:table1_limit]
    else:
        result = result[:table1_limit]
    result_set.append(result)




