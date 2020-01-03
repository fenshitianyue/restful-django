#!/usr/bin/env python
# -*- coding: utf-8 -*-

class SqlQuery:
    def __init__(self):
        self.sql = ''
        self.sql_select = ['select']  # 对应 fields
        self.sql_from = ['from']
        self.sql_where = ['where']  # 对应 filter
        self.sql_group_by = ['group by']
        self.sql_order_by = ['order by']  # 对应 sort
        self.sql_limit = ['limit', '10000']


def parse_json_to_sql(raw_query=None):
    query = SqlQuery()
    if raw_query.get('from'):
        query.sql_from.append(raw_query['from'])

    # 子语法块中的fields不用考虑别名，别名统一放到顶层字段fields中处理
    if raw_query.get('fields'):
        for field in raw_query['fields']:
            if '__' in field:
                continue
            else:
                query.sql_select.append('{},'.format(field))

    if raw_query.get('group_by'):
        for field in raw_query['group_by']:
            query.sql_group_by.append('{},'.format(field))
        query.sql_group_by[-1] = query.sql_group_by[-1][:-1]  # 去掉最后的','

        if raw_query.get('aggregation'):
            func_map = {
                'count':            lambda query, field: query.sql_select.append('count({0}) as {0}__count,'.format(field)),
                'sum':              lambda query, field: query.sql_select.append('sum({0}) as {0}__sum,'.format(field)),
                'avg':              lambda query, field: query.sql_select.append('avg({0}) as {0}__avg'.format(field)),
                'count_distinct':   lambda query, field: query.sql_select.append('count(distinct {0}) as {0}__count_distinct'.format(field))
            }
            for field in raw_query['aggregation']:
                field_name, op = field.split('__')[0], field.split('__')[1]
                if op in func_map.keys():
                    func_map[op](query, field_name)
                else:
                    pass  # raise exception: not support this aggregation option
        else:
            pass  # raise exception: synatx error
    else:
        query.sql_select[-1] = query.sql_select[-1][:-1]  # 去掉 select 末尾的 ','

    if raw_query.get('filter'):
        pass

    if raw_query.get('sort'):
        for field in raw_query['sort']:
            if field.startswith('-'):
                query.sql_order_by.append('{} desc,'.format(field))
            else:
                query.sql_order_by.append('{},'.format(field))
        query.sql_order_by[-1] = query.sql_order_by[-1][:-1]  # 去掉最后的','

    if raw_query.get('limit') and raw_query['limit'] < int(query.sql_limit[1]):
        query.sql_limit[1] = str(raw_query['limit'])

    tmp = query.sql_select + query.sql_from + query.sql_group_by + query.sql_order_by + query.sql_limit
    query.sql = ' '.join(tmp)
    print query.sql


json_query = {
    "select": {
        "from": "table1",
        # "filter": {"field1": "value1", "field2__gt": 5, "field3__contains": "substr1"},  # "field4__startswith"}
        "group_by": ["field1", "field2"],
        "aggregation": ["field3__count", "field5__count_distinct"],
        "sort": ["field1", "-field2"],
        "fields": ["field1", "field2", "field3__count", "field5__count_distinct@f5_cnt"],
        "limit": 500
    }
}

parse_json_to_sql(json_query['select'])




