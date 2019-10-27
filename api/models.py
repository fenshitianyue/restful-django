# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# Create your models here.

class Test1(models.Model):
    field1 = models.CharField(max_length=10)
    field2 = models.IntegerField()
    field3 = models.CharField(max_length=20)
    field4 = models.IntegerField()
    field5 = models.CharField(max_length=10)

class Test2(models.Model):
    field1 = models.CharField(max_length=10)
    field2 = models.IntegerField()
    field3 = models.CharField(max_length=20)

class Test3(models.Model):
    field1 = models.CharField(max_length=10)
    field2 = models.IntegerField()
    field3 = models.CharField(max_length=20)


