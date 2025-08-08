import requests 

url = "https://hackathon.omelet.tech/api/maps/"
response = requests.get(url)

if response.status_code == 200:
    json_data = response.json()
    
    if "results" in json_data and len(json_data["results"]) > 0:
        result_0 = json_data["results"][0]
        print(result_0)
    else:
        print("Không có dữ liệu trong 'results'")
else:
    print(f"Không thể lấy dữ liệu. Mã trạng thái: {response.status_code}")
