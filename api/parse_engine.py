# -*- coding:UTF-8 -*-

import api.models
import django.db.models
import pandas as pd

def get_model(model_name):
    return getattr(api.models, model_name)

def get_attribute(attribute_name):
    return getattr(django.db.models, attribute_name)

def query_func(select, result_set):
    table = get_model(select['from'])
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
            table_limit = select['limit']
        else:
            table_limit = 10000
    else:
        table_limit = 10000

    if had_filter and had_agg:
        result = table.objects.values(*select['group_by']).filter(**select['filter']).annotate(**agg_dict)
    elif not had_filter and had_agg:
        result = table.objects.values(*select['group_by']).annotate(**agg_dict)
    elif had_filter and not had_agg:
        result = table.objects.values(*select['group_by']).filter(**select['filter'])
    else:
        pass

    if select.get('order_by') is not None:
        result = result.order_by(*select['order_by'])[:table_limit]
    else:
        result = result[:table_limit]
    result_set.append(result)

def from_json_get_result(query):
    select = dict()
    result_set = list()
    joins_type = list()
    # 先拿出select字段
    if query.get('select'):
        select = query['select']
    query_func(select, result_set)
    # 再拿出join字段，分别计算后将计算结果添加到集合中
    if query.get('join') is not None:
        for join_it in query['join']:
            joins_type.append(join_it['type'])
            query_func(join_it['query']['select'], result_set)
    print '-----------------------------'
    ####################################
    # for it in result_set:
    #     print it.query
    #     print '-----------------------------'
    ####################################
    # 进行join
    df_main = pd.DataFrame(list(result_set[0]))
    fields = list() # TODO:
    for it in joins_type:
        if it == 'inner_join':
            df_main = pd.merge(df_main, on=fields)
        elif it == 'left_join':
            df_main = pd.merge(df_main, on=fields, how='left')
        elif it == 'full_join':
            df_main = pd.merge(df_main, on=fields, how='outer')
    # 对join结果排序
    if query.get('sort'):
        sort_list = query['sort']
        sort_methods = list()
        for it in sort_list:
            if it.find('_') != -1:
                it = it[it.find('_'):]
                sort_methods.append('False')
            else:
                sort_methods.append('True')
        df_main.sort_values(by=sort_list, ascending=sort_methods)
    # 对join结果分片
    if query.get('limit'):
        if query['limit'] < 10000:
            df_main = df_main[0:query['limit']]
        else:
            df_main = df_main[0:10000]
    else:
        df_main = df_main[0:10000]









