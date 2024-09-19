# stockapi/urls.py
from django.urls import path
from .views import stock_summary

urlpatterns = [
    path('stock/', stock_summary, name='stock_summary'),
]