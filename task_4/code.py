import pyarrow.parquet as pq
import requests
import os
import psutil
import time

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def download_images(link, output_folder, num_images):
    if not os.path.exists("links.parquet"):
        print("downloading parquet file...")
        download_file(link, "links.parquet")

    table = pq.read_table("links.parquet")
    links = table.to_pandas()

    os.makedirs(output_folder, exists_ok = True)

    for i, link  in enumerate(links['image_url'][:num_images]):
        filename = os.path.join(output_folder, f"image_{i}.jpg")
        if not os.path.exists(filename):
            print(f"downloading {filename}")
            response = request.get(link, stream =True)
            with open(filename, 'wb') as f:
                for chunk in response.iter_count(chunk_size = 1024):
                    f.write(chunk)
        else:
            print(f"{filename} already exists, skipping. ")

def monitor_performance(interval):
    while true:

        cpu_percent = psutil.cpu_percent(interval=None)

        print(f"cpu usage : {cpu_percent}%")


        #network usage

        net_usage = psutil.net_io_counters()
        print(f"Network Usage - Sent: {net_usage.bytes_sent} bytes, Received: {net_usage.bytes_recv} bytes")

        
           
