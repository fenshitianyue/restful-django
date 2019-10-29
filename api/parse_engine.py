# -*- coding:UTF-8 -*-

import api.models
import django.db.models
import json
from querybuilder.query import Query
import querybuilder.query
import querybuilder.fields

def get_model(model_name):
    return getattr(api.models, model_name)

def get_fields(field_list):
    fields = list()
    # fields.append('field1')
    for it in field_list:
        if it.find('sum') != -1 or it.find('avg') != -1 or it.find('count') != -1:
            continue
        fields.append(it)
    return fields


# version1:测试join
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
        arguments = list()
        fields = get_fields(select['fields'])
        for it in select['aggregation']:
            # 收集fields中单独的field
            field = it[:it.find('_')]
            if it.find('count') != -1:
                if it.find('distinct') != -1:
                    fields.append(getattr(querybuilder.query, 'CountField')(field, alias=it, distinct=True))
                else:
                    fields.append(getattr(querybuilder.query, 'CountField')(field, alias=it))
            if it.find('sum') != -1:
                if it.find('distinct') != -1:
                    fields.append(getattr(querybuilder.query, 'SumField')(field, alias=it, distinct=True))
                else:
                    fields.append(getattr(querybuilder.query, 'SumField')(field, alias=it))
            if it.find('avg') != -1:
                if it.find('distinct') != -1:
                    fields.append(getattr(querybuilder.query, 'AvgField')(field, alias=it, distinct=True))
                else:
                    fields.append(getattr(querybuilder.query, 'AvgField')(field, alias=it))
        arguments.append(fields)
        query = Query().from_table(table1, *arguments).where(**select['filter'])
        if select.get('group_by') is not None:
            for it in select['group_by']:
                query = query.group_by(it)
        if select.get('order_by') is not None:
            for it in select['order_by']:
                query = query.order_by(it)

        # 多表解析
        table2 = getattr(api.models, 'Test2')
        p_list = list()
        fields = list(['field1', 'field2'])
        fields.append(getattr(querybuilder.query, 'CountField')('field3', alias='field3__count'))
        p_list.append(fields)
        filters = dict({'field1': 'Test1', 'field2__gt': 5, 'field3__contais': 'world'})

        query2 = Query().from_table(table2, *p_list).where(**filters)
        print query2.get_sql()
        # query.join(right_table=query2)
        conditions = 'api_test1.field1=api_test2.field1 AND api_test1.field2=api_test2.field2'
        query.join(right_table='Test2', condition=conditions, join_type='left join')

        print '----------------------------------------------'
        print query.get_sql()
        print '----------------------------------------------'
        # query.select()


    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
