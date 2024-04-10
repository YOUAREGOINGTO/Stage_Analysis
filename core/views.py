from django.shortcuts import render
from django.http import JsonResponse
from django.core.files.storage import FileSystemStorage
from .utils import get_stock_data,stock_list
import os
from django.conf import settings


def index(request):
    return render(request, 'index.html')

def get_stock_data_view(request):
    if request.method == 'POST':
        try:
            uploaded_file = request.FILES.get('file')
            interval = request.POST.get('interval')
            print(interval)
            if not uploaded_file:
                return JsonResponse({'error': 'No file uploaded'}, status=400)
            
            # Get the file name
            file_name = uploaded_file.name
            print(file_name)
            
            # Check if a file with the same name exists in the media root
            media_root = settings.MEDIA_ROOT
            
            #print(media_root)
            file_path = os.path.join(media_root, file_name)
            print(file_path)
            if os.path.exists(file_path):
                os.remove(file_path)  # Delete the old file
            
            # Save the uploaded file to the media root
            fs = FileSystemStorage(location=media_root)
            fs.save(file_name, uploaded_file)
            
            

            # Call your Python function to get stock data
            stock_data = get_stock_data(file_path, interval=interval)
            
            return JsonResponse(stock_data.to_dict(orient='records'),safe = False)
            
        except Exception as e:
            print(e)
            return JsonResponse({'error': str(e)}, status=400)
    else:
        return JsonResponse({'error': 'Method not allowed'}, status=405)
