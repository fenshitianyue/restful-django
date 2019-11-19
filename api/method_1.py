# -*- coding:UTF-8 -*-

# import django.db.models
import pandas as pd


def get_limit(limit):
    if limit is not None:
        if limit < 10000:
            return limit
        else:
            return 10000
    else:
        return 10000

def transfer_json_to_sql(select_field):
    raw_sql = 'select '
    if select_field.get('from') is None:
        raise RuntimeError('not found "from" field in "select" field')
    # 基础字段：from、fields
    # 收集 from 字段
    from_field = 'from ' + select_field['from']
    # 收集 field 字段
    fields_field = ''

    # fields 为空，aggregtion也为空，默认搜索所有字段
    # fields 不为空，aggregation为空，简单拼接，不需要解析
    # fields 不为空，aggregation也不为空，复杂拼接，需要解析
    # fields 为空，aggregation 不为空，抛出异常
    if select_field.get('fields') is None and select_field.get('aggregation') is None:  # 默认情况
        fields_field = '* '
    elif select_field.get('fields') is not None and select_field.get('aggregation') is None:  # 简单拼接
        for it in select_field['fields']:
            fields_field = fields_field + select_field['from'] + '.' + it + ' '
    elif select_field.get('fields') is not None and select_field.get('aggregation') is None:  # 复杂拼接

        pass
    else:  # 异常情况
        raise RuntimeError('check your query')
    raw_sql += fields_field + from_field
    print '-----------------'
    print raw_sql
    print '-----------------'

    # 收集 filter 字段
    if select_field.get('filter') is not None:
        pass

    if select_field.get('group_by') is None and select_field.get('aggregation') is not None:
        raise RuntimeError('"group_by" must be used with the "aggregation" field')
    # 收集 group by 字段

    # 收集 limit 字段
    # 收集 sort 字段

def transfer_sql_to_dsl():
    pass

def es_query(query):
    select_field = query.get('select')
    # 将json格式的输入转换为sql
    if select_field is None:
        raise RuntimeError('"select" field not found!')
    main_sql = {
        'join_type': 'null',
        'sql': ''
    }
    main_sql['sql'] = transfer_json_to_sql(select_field)
    sql_list = [main_sql]

    if query.get('join') is not None:
        for join_it in query['join']:
            sql_item = {
                'join_type': join_it['type'],
                'join_on': [],
                'sql': ''
            }
            for key in join_it['on'].keys():
                sql_item['join_on'].append(key)
            sql_item['sql'] = transfer_json_to_sql(join_it['query']['select'])
            sql_list.append(sql_item)


    # TODO:调用elasticsql将sql转化为es dsl
    # TODO:将每个dsl查询结果收集起来
    # TODO:将es的查询结果根据join类型在内存中join
    # TODO:将join结果组织一下返回给前端
