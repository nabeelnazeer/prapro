"""
Download links.parquet from https://drive.google.com/file/d/1ym_RbsjN41cwXZLeB3NibN7h7Vbz1AgP/view?usp=sharing

Download and save the first 10,000 images from the links in the parquet file.

- Monitor CPU and network usage during download.

References:
pyarrow package
requests package

"""

import os 
import pyarrow as pq
import requests

class Image_Downloader:

    def __init__(self, parquet_url):
        self.parquet_url = parquet_url

    def download_images():    