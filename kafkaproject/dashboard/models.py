from django.db import models

class category1(models.Model):
    category1 = models.TextField(null=True, blank=True)
    count = models.IntegerField(null=True, blank=True)

class category2(models.Model):
    category2 = models.TextField(null=True, blank=True)
    count = models.BigIntegerField(null=True, blank=True)

class page(models.Model):
    page = models.TextField(null=True, blank=True)
    count = models.BigIntegerField(null=True, blank=True)

class ip(models.Model):
    ip = models.TextField(null=True, blank=True)
    count = models.BigIntegerField(null=True, blank=True)
