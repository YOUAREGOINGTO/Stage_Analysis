"""
URL configuration for  project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
# In your project's views.py
from django.shortcuts import redirect

# In your project's urls.py
from django.urls import path, include
from core import views  # Import views from the core app
from django.contrib import admin
from django.urls import path, include
from core import views  # Import views from the core app

urlpatterns = [
    path('', views.index, name='root'),  # Use views from the core app
    path('admin/', admin.site.urls),
    path('core/', include('core.urls')),
]




