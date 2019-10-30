# -*- coding:UTF-8 -*-

import api.models
import django.db.models
import pandas as pd

def get_model(model_name):
    return getattr(api.models, model_name)

def get_attribute(attribute_name):
    return getattr(django.db.models, attribute_name)

def get_limit(limit):
    if limit is not None:
        if limit < 10000:
            return limit
        else:
            return 10000
    else:
        return 10000

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

    table_limit = get_limit(select.get('limit'))

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
    result_set.remove(result_set[0])
    fields = list(['field1', 'field2'])  # TODO:

    index = 0
    for it in result_set:
        it = pd.DataFrame(list(it))
        if joins_type[index] == 'inner_join':
            df_main = df_main.merge(it, on=fields)
        elif joins_type[index] == 'left_join':
            df_main = df_main.merge(it, on=fields, how='left')
        elif joins_type[index] == 'full_join':
            df_main = df_main.merge(it, on=fields, how='outer')
        else:
            pass
        index += 1

    # 对join结果排序
    if query.get('sort') is not None:
        sort_list = query['sort']
        sort_methods = list()
        for it in sort_list:
            if it.find('-') != -1:
                it = it[it.find('-'):]
                sort_methods.append('False')
            else:
                sort_methods.append('True')
        df_main.sort_values(by=sort_list, ascending=sort_methods)
    # 对join结果分片

    df_main = df_main[-1:get_limit(query.get('limit'))]

    print df_main.head()
    print '-----------------------------'

