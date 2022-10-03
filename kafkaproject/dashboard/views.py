from django.db.models import Count
from django.http import HttpResponse
from django.http import JsonResponse
from .models import category1, category2, page, ip
from django.core import serializers
from django.shortcuts import render

def dashboard(request):
    return render(request, 'dashboard.html', {})

def dashboard_data(request):
    dataset = category1.objects.all()
    data = serializers.serialize('json', dataset)
    dataset1 = page.objects.annotate(countData=Count('count')).order_by('-count')[:5]
    data1 = serializers.serialize('json', dataset1)
    dataset2 = ip.objects.annotate(countData=Count('count')).order_by('-count')[:5]
    data2 = serializers.serialize('json', dataset2)
    dataset3 = category2.objects.annotate(countData=Count('count')).order_by('-count')[:5]
    data3 = serializers.serialize('json', dataset3)
    return JsonResponse([data, data1, data2, data3], safe=False)
