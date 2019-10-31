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
    has_filter, has_agg = False, False
    if select.get('filter') is not None:
        has_filter = True
    if select.get('aggregation') is not None:
        has_agg = True
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

    if has_filter and has_agg:
        if select.get('group_by') is not None:
            result = table.objects.values(*select['group_by']).filter(**select['filter']).annotate(**agg_dict)
        else:
            result = table.objects.filter(**select['filter']).annotate(**agg_dict)  # TODO:这里可能有bug
    elif not has_filter and has_agg:
        if select.get('group_by'):
            result = table.objects.values(*select['group_by']).annotate(**agg_dict)
        else:
            result = table.objects.annotate(**agg_dict)
    elif has_filter and not has_agg:
        """
        没有聚合函数，有或者没有group_by字段其实都无所谓
        如果要更严谨的话，进入这个分支后，判断group_by的字段包含所查询表的所有列，若没有，则报错
        这里暂时从简处理
        """
        result = table.objects.filter(**select['filter'])
    else:
        """
        这里同上
        """
        result = table.objects.all()

    if select.get('order_by') is not None:
        result = result.order_by(*select['order_by'])[:table_limit]
    else:
        result = result[:table_limit]
    result_set.append(result)

def from_json_get_result(query):
    result_set = list()
    join_types = list()
    # 先拿出select字段
    if query.get('select'):
        query_func(query['select'], result_set)
    # 再拿出join字段，分别计算后将计算结果添加到集合中
    join_fields = list()
    if query.get('join') is not None:
        for join_it in query['join']:
            join_types.append(join_it['type'])
            query_func(join_it['query']['select'], result_set)
            # 收集join连接条件字段
            tmp = list()
            for key in join_it['on'].keys():
                tmp.append(key)
            join_fields.append(tmp)
    print '-----------------------------'
    for it in result_set:
        print it.query
        print '-----------------------------'
    # 进行join
    # df_main = pd.DataFrame(list(result_set[0]))
    # result_set.remove(result_set[0])
    # index = 0
    # for it in result_set:
    #     if not it.exists():
    #         continue
    #     it = pd.DataFrame(list(it))
    #     if join_types[index] == 'inner_join':
    #         df_main = df_main.merge(it, on=join_fields[index])
    #     elif join_types[index] == 'left_join':
    #         df_main = df_main.merge(it, on=join_fields[index], how='left')
    #     elif join_types[index] == 'full_join':
    #         df_main = df_main.merge(it, on=join_fields[index], how='outer')
    #     else:
    #         pass
    #     index += 1

    # # 对join结果排序
    # if query.get('sort') is not None:
    #     sort_list = query['sort']
    #     sort_methods = list()
    #     for it in sort_list:
    #         if it.find('-') != -1:
    #             it = it[it.find('-'):]
    #             sort_methods.append('False')
    #         else:
    #             sort_methods.append('True')
    #     df_main.sort_values(by=sort_list, ascending=sort_methods)

    # # 对join结果分片
    # df_main = df_main[0:get_limit(query.get('limit'))]

    # print df_main.head()
