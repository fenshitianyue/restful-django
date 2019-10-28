# -*- coding:UTF-8 -*-

import api.models
import django.db.models
from django.core.serializers import serialize
import json
from querybuilder.query import Query
import querybuilder.query

def get_model(model_name):
    return getattr(api.models, model_name)

def get_fields(field_list):
    fields = list()
    for it in field_list:
        if it.find('sum') != -1 or it.find('avg') != -1 or it.find('count') != -1:
            continue
        fields.append(it)
    return fields


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
        for it in select['aggregation']:
            # 收集fields中单独的field
            fields = get_fields(select['fields'])
            fields.append(getattr(querybuilder.query, 'SumField')('field4', alias='field4__sum'))
            fields.append(getattr(querybuilder.query, 'AvgField')('field4', alias='field4__avg'))
            fields.append(getattr(querybuilder.query, 'CountField')('field5', alias='field5__count_distinct', distinct=True))
            para_list = list()
            para_list.append(fields)
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

        query = Query().from_table(table1, *para_list).where(**select['filter'])

        print '----------------------------------------------'
        print query.get_sql()
        print '----------------------------------------------'

    # if result.exists():
    #     pass
    #     # serialize_data = serialize('json', result)
    #     # print serialize_data
    #     # print result[0].field1
    #     # print result[0].field3__count
    #     # for it in result:
    #     #     print it.field3__count
    #     #     print it.field4__sum
    #     #     print it.field4__avg
    #     #     print it.field5__count_distinct
    # else:
    #     print "not found"
    # TODO:将结果构造成一个字典，然后返回json.dumps(dict)
