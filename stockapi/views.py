# stockapi/views.py
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from datetime import datetime, timedelta
import pandas as pd

@require_http_methods(["GET"])
def stock_summary(request):
    """
    Retrieves and returns the top 30 ranked stocks from the latest saved data file.
    """

    # Define base directory for saved data
    base_dir = "/Users/kartikaypandey/Documents/django_projects/stockproject/top50_"

    # Get today's date string
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Construct file path
    file_path = f"{base_dir}{today_str}.csv"

    # Try to read data from the file
    try:
        # Read data from CSV using pandas
        data = pd.read_csv(file_path)
    except FileNotFoundError:
        # Handle case where file doesn't exist
        return JsonResponse({"message": "No data available yet."}, status=404)
    except Exception as e:
        # Handle other errors
        return JsonResponse({"message": f"Error retrieving data: {e}"}, status=500)

    # Convert pandas DataFrame to a list of dictionaries
    data = data.to_dict(orient='records')

    return JsonResponse(data, safe=False)