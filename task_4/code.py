import pyarrow.parquet as pq
import requests
import os
import psutil
import time
import threading

def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def download_images(link, output_folder, num_images):
    if not os.path.exists("links.parquet"):
        print("Downloading parquet file...")
        download_file(link, "links.parquet")

    table = pq.read_table("links.parquet")
    links = table.to_pandas()

    os.makedirs(output_folder, exist_ok=True)

    for i, img_link in enumerate(links['image_url'][:num_images]):
        filename = os.path.join(output_folder, f"image_{i}.jpg")
        if not os.path.exists(filename):
            print(f"Downloading {filename}")
            response = requests.get(img_link, stream=True)
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
        else:
            print(f"{filename} already exists, skipping.")

def monitor_performance(interval):
    while True:
        cpu_percent = psutil.cpu_percent(interval=None)
        print(f"CPU Usage: {cpu_percent}%")

        net_io = psutil.net_io_counters()
        print(f"Network Usage - Sent: {net_io.bytes_sent} bytes, Received: {net_io.bytes_recv} bytes")

        time.sleep(interval)

def main():
    link = "https://drive.google.com/uc?export=download&id=1ym_RbsjN41cwXZLeB3NibN7h7Vbz1AgP"
    output_folder = "downloaded_images"
    num_images = 1000

    monitor_interval = 10
    monitor_thread = threading.Thread(target=monitor_performance, args=(monitor_interval,))

    monitor_thread.start()

    download_images(link, output_folder, num_images)

    monitor_thread.terminate()

if __name__ == "__main__":
    main()
