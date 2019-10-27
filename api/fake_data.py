# -*- coding:UTF-8 -*-

from api.models import Test1
from api.models import Test2
from api.models import Test3


def to_test1(data):
    try:
        tmp = Test1(field1=data['field1'], \
                    field2=data['field2'], \
                    field3=data['field3'], \
                    field4=data['field4'], \
                    field5=data['field5'])
        tmp.save()
    except Exception as e:
        print 'fake_data.to_test1 error: %s' % e.message
        return False
    return True

def to_test2(data):
    try:
        tmp = Test2(field1=data['field1'], \
                    field2=data['field2'], \
                    field3=data['field3'])
        tmp.save()
    except Exception as e:
        print 'fake_data.to_test2 error: %s' % e.message
        return False
    return True

def to_test3(data):
    try:
        tmp = Test2(field1=data['field1'], \
                    field2=data['field2'], \
                    field3=data['field3'])
        tmp.save()
    except Exception as e:
        print 'fake_data.to_test2 error: %s' % e.message
        return False
    return True

