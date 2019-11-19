# -*- coding:UTF-8 -*-

import django.db.models
import pandas as pd
from init_model_set import *
import model_set

def get_model(model_name):
    return getattr(model_set, table_map[model_name])

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

def pg_query_func(select, result_set):
    table = get_model(select['from'])
    has_filter, has_agg = False, False
    if select.get('filter') is not None:
        has_filter = True
    if select.get('aggregation') is not None:
        has_agg = True
        agg_dict = dict()
        for it in select['aggregation']:
            field = it[:it.find('__')]
            if it.find('__count') != -1:
                if it.find('_distinct') != -1:
                    agg_dict.update({it: get_attribute('Count')(field, distinct=True)})
                else:
                    agg_dict.update({it: get_attribute('Count')(field)})
            if it.find('__sum') != -1:
                if it.find('_distinct') != -1:
                    agg_dict.update({it: get_attribute('Sum')(field, distinct=True)})
                else:
                    agg_dict.update({it: get_attribute('Sum')(field)})
            if it.find('__avg') != -1:
                if it.find('_distinct') != -1:
                    agg_dict.update({it: get_attribute('Avg')(field, distinct=True)})
                else:
                    agg_dict.update({it: get_attribute('Avg')(field)})

    table_limit = get_limit(select.get('limit'))

    if has_filter and has_agg:
        if select.get('group_by') is not None:
            result = table.objects.values(*select['group_by']).filter(**select['filter']).annotate(**agg_dict)
        else:
            result = table.objects.filter(**select['filter'].annotate(**agg_dict)).values(*select['fields'])
    elif not has_filter and has_agg:
        if select.get('group_by'):
            result = table.objects.values(*select['group_by']).annotate(**agg_dict)
        else:
            result = table.objects.annotate(**agg_dict).values(*select['fields'])
    elif has_filter and not has_agg:
        for it in select['fields']:
            if it.find('__sum') != -1 or it.find('__avg') != -1 or it.find('__count') != -1:
                raise SyntaxError('Check the "fields" field in the query')
        result = table.objects.filter(**select['filter']).values(*select['fields'])
    else:
        for it in select['fields']:
            if it.find('__sum') != -1 or it.find('__avg') != -1 or it.find('__count') != -1:
                raise SyntaxError('Check the "fields" field in the query')
        result = table.objects.all().values(*select['fields'])

    if select.get('order_by') is not None:
        result = result.order_by(*select['order_by'])[:table_limit]
    else:
        result = result[:table_limit]
    result_set.append(result)

def pg_query(query):
    result_set = list()
    join_types = list()
    # 先拿出select字段
    if query.get('select'):
        pg_query_func(query['select'], result_set)
    # 再拿出join字段，分别计算后将计算结果添加到集合中
    join_fields = list()
    if query.get('join') is not None:
        for join_it in query['join']:
            join_types.append(join_it['type'])
            pg_query_func(join_it['query']['select'], result_set)
            # 收集join连接条件字段
            tmp = list()
            for key in join_it['on'].keys():
                tmp.append(key)
            join_fields.append(tmp)
    print '-----------------------------'
    # raw_sql = str(result_set[0].query)
    # print raw_sql

    # 进行join
    df_main = pd.DataFrame(list(result_set[0]))
    result_set.remove(result_set[0])
    index = 0
    for it in result_set:
        if not it.exists():
            continue
        it = pd.DataFrame(list(it))
        if join_types[index] == 'inner_join':
            df_main = df_main.merge(it, on=join_fields[index])
        elif join_types[index] == 'left_join':
            df_main = df_main.merge(it, on=join_fields[index], how='left')
        elif join_types[index] == 'full_join':
            df_main = df_main.merge(it, on=join_fields[index], how='outer')
        else:
            raise SyntaxError('Not support :"%s"' % join_types[index])
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
    df_main = df_main[0:get_limit(query.get('limit'))]
    df_main = df_main.fillna('')

    print df_main.head()
    print '-----------------------------'
    # 保存查出来的表
    value = list()
    for row in df_main.itertuples():
        line = list()
        for i in range(len(row) - 1):
            line.append(row[i+1])
        value.append(line)
    # for line in value:
    #     print line

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
            fields_field += select_field['from'] + '.' + it + ','
    elif select_field.get('fields') is not None and select_field.get('aggregation') is not None:  # 复杂拼接
        for it in select_field['aggregation']:
            field = it[:it.find('__')]
            if it.find('__count') != -1:
                if it.find('_distinct') != -1:
                    fields_field += 'count(distinct ' + field + ') ' + 'as '
                else:
                    fields_field += 'count(' + field + ') ' + 'as '
            if it.find('__sum') != -1:
                if it.find('_distinct') != -1:
                    fields_field += 'sum(distinct ' + field + ') ' + 'as '
                else:
                    fields_field += 'sum(' + field + ') ' + 'as '
            if it.find('__avg') != -1:
                if it.find('_distinct') != -1:
                    fields_field += 'avg(distinct ' + field + ') ' + 'as '
                else:
                    fields_field += 'avg(' + field + ') ' + 'as '
            # 解析出as后面的内容
            for field_it in select_field['fields']:
                if field in field_it:
                    pos = field_it.find('@')
                    if pos != -1:
                        fields_field += field_it[pos:] + ','
                    else:
                        fields_field += field_it + ','
    else:  # 异常情况
        raise RuntimeError('check your query')
    fields_field = fields_field[0:-1] + ' '
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
