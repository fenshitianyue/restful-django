# -*- coding:UTF-8 -*-

import api.models
import django.db.models
import json
import pandas as pd
# from django.core.paginator import Paginator
# from djcompoundqueryset import CompoundQueryset

def get_model(model_name):
    return getattr(api.models, model_name)

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

    table_limit = 200
    result = table1.objects.values(*select['group_by']).filter(**select['filter']).annotate(**agg_dict).order_by(*select['order_by'])[:table_limit]
    ####################################################
    # test multiple-table
    table2 = getattr(api.models, 'Test2')
    fields = ['field1', 'field2']
    filters = {'field1': 'test1', 'field2__gt': 5, 'field3__contains': 'hello'}
    agg_dict = {'field3__count': getattr(django.db.models, 'Count')('field3')}
    result2 = table2.objects.values(*fields).filter(**filters).annotate(**agg_dict)[:table_limit]
    print '------------------------------------------------------'
    print result2.query
    print '------------------------------------------------------'
    print result.query
    print '------------------------------------------------------'
    # combinded_queryset = CompoundQueryset(result, result2)
    # result3 = Paginator(combinded_queryset, 10)
    # print type(result3)
    # print result3
    # print '------------------------------------------------------'

    df1 = pd.DataFrame(list(result))
    df2 = pd.DataFrame(list(result2))

    result = df1.merge(df2, on=fields)
    print df1.head()
    print '------------------------------------------------------'
    print df2.head()
    print '------------------------------------------------------'
    print result.head()
    print '------------------------------------------------------'

    ####################################################

    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
