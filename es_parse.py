# -*- coding:UTF-8 -*-
# import kutil.django_models_init
# from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from django.conf import settings


class RestSQLQuery(object):
    q_from = ""
    q_filter = dict()
    q_group_by = []
    q_fields = []
    q_aggregation = []
    q_sort = []
    q_limit = 2000


class EsClient(object):
    def __init__(self, es_url):
        self.es_url = es_url
        # self.client = Elasticsearch(self.es_url.split(','))

    def query(self, query):
        dsl = {
            "size": query.q_limit,
            "query": {
                "bool": {
                    "must": []
                }
            },
            "sort": [],
            "_source": {
                "includes": []
            },
            "aggs": {
                "groupby": {
                    "terms": {
                        "script": {
                            "source": ""
                        }
                    },
                    "aggs": {}
                }
            }
            # "aggs": {
            #     "groupby": {
            #         "composite": {  # composite 是6.x及以上版本的引入的语法
            #             "size": query.q_limit,
            #             "sources": []
            #         },
            #         "aggs": {}
            #     }
            # }
        }

        dsl["_source"]["includes"] = query.q_fields
        dsl_where = dsl["query"]["bool"]["must"]
        dsl_group_by = ""
        dsl_group_aggs = dsl["aggs"]["groupby"]["aggs"]
        dsl_sort = dsl['sort']
        # s = Search(using=self.client, index=q.q_from)

        # 处理 filter
        for field, value in q.q_filter.items():
            if "__" not in field:  # 完全匹配
                dsl_where.append({
                    "term": {
                      field: value
                    }
                  })
            else:
                op = field.split("__")[1]
                field_name = field.split("__")[0]
                if op == "gt":
                    dsl_where.append({
                      "range": {
                        field_name: {"gt": value}
                      }
                    })
                elif op == "lt":
                    dsl_where.append({
                      "range": {
                        field_name: {"lt": value}
                      }
                    })
                elif op == "gte":
                    dsl_where.append({
                        "range": {
                            field_name: {"gte": value}
                        }
                    })
                elif op == "lte":
                    dsl_where.append({
                        "range": {
                            field_name: {"lte": value}
                        }
                    })
                elif op == "contains":
                    dsl_where.append({
                        "match_phrase": {
                            field_name: {
                                "query": value
                            }
                        }
                    })
                elif op == 'startswith':
                    dsl_where.append({
                        "prefix": {field_name: value}
                        }
                    )
                elif op == 'endswith':
                    dsl_where.append({
                        "wildcard": {field_name: ''.join(['*', value])}
                    })
                elif op == "range":
                    dsl_where.append({
                        "range": {  # 这里预估value是一个只包含两个元素的列表
                            field_name: {"gte": value[0], "lte": value[1]}
                        }
                    })
                elif op == "in":
                    dsl_where.append({
                        "terms": {
                            field_name: value  # 这里预估value是一个列表
                        }
                    })
                else:
                    raise ValueError("can not accept op: %s, field: %s" % (op, field))
        if query.q_group_by:
            # process group by
            print 'enter group by'
            for field in query.q_group_by:
                dsl_group_by = ''.join([dsl_group_by, "doc['", field, "'].value", '-'])

            dsl_group_by = dsl_group_by[:len(dsl_group_by)-1]
            dsl["aggs"]["groupby"]["terms"]["script"]["source"] = dsl_group_by

            # process aggregation
            for field in query.q_aggregation:
                field_name, op = field.split("__")[0], field.split("__")[1]
                func_map = {"count": "value_count", "sum": "sum", "avg": "avg", "max": "max", "min": "min"}
                if op in func_map:
                    dsl_group_aggs[field] = {func_map[op]: {"field": field_name}}
                elif op.find("_distinct") != -1:
                    dsl_group_aggs["count"] = {"cardinality": {"field": field_name}}
                else:
                    raise ValueError("can not accept aggregation func: " + op)
        else:
            del dsl["aggs"]

        # process sort
        if query.q_sort:
            for sort_it in query.q_sort:
                is_reverse = sort_it.find('-')
                if is_reverse != 0:
                    dsl_sort.append({
                        sort_it: {"order": "asc"}
                    })
                else:
                    field = ''.join(sort_it.split('-')[1:])
                    dsl_sort.append({
                        field: {"order": "desc"}
                    })
        else:
            del dsl['sort']
        print dsl
        # response = self.client.search(index=query.q_from, body=dsl)
        # return self.response_to_records(response)

    @staticmethod
    def response_to_records(response):
        records = []
        if "aggs" in response:
            records = response["aggs"]["groupby"]["buckets"]
            for r in records:
                r.update(r["key"])
                del r["key"]
                for k, v in r.items():
                    if isinstance(v, dict) and "value" in v:
                        r[k] = v["value"]
        elif "hits" in response and "hits" in response["hits"]:
            records = list(map(lambda x: x["_source"], response["hits"]["hits"]))
        print("response.records", records)
        return records


if __name__ == "__main__":
    # es = EsClient(settings.ATHENA_ES_URL)
    es = EsClient('url')
    q = RestSQLQuery()
    q.q_from = "tencent.metrics-daily-20191104"
    # q.q_filter = {
    #     "category": "PO.MODEL_OS_STAT",
    #     "value__gt": 1000,
    #     # "field1__startswith": "world",
    #     # "field2__endswith": "world",
    #     "p_id__in": [1, 2, 3, 4, 5]
    # }
    q.q_group_by = ["time", "app_id"]
    q.q_aggregation = ["value__sum", "cnt__count", "dev_id__count_distinct"]
    q.q_fields = ["app_id", "version", "id"]
    # q.q_sort = ['field5', '-field6']
    q.q_limit = 10
    es.query(q)


