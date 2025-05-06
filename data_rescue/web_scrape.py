# %%
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from urllib.parse import urljoin
import time

# Setup headless Chrome
options = Options()
options.headless = True
driver = webdriver.Chrome(options=options)

url = "https://www.bls.gov/iif/fatal-injuries-tables/archive.htm#DATA"
driver.get(url)
time.sleep(5)  # Wait for page to load

# Get all downloadable file links
links = driver.find_elements("tag name", "a")
file_exts = (".pdf", ".xls", ".xlsx", ".csv")
file_urls = []

for link in links:
    href = link.get_attribute("href")
    if href and href.lower().endswith(file_exts):
        file_urls.append(href)

driver.quit()

# Print results
print(f"Found {len(file_urls)} files:")

base_dir = 'fatal_injuries_2'
import requests
import certifi
import os
import re


def download(url):
	file_name = url.split('/')[-1]

	name_without_ext = file_name.split('.')[0]

	# Default year folder
	year_folder = "Miscellaneous"
	year_range_pattern = re.compile(r'(\d{4})[-–](\d{2,4})')
	single_year_pattern = re.compile(r'(\d{4})')

	# Try to extract year range first
	match_range = year_range_pattern.search(name_without_ext)
	if match_range:
		start_year = int(match_range.group(1))
		end_part = match_range.group(2)
		# If end_part is 2 digits, convert to full year
		end_year = int(end_part) if len(end_part) == 4 else int(str(start_year)[:2] + end_part)
		year_folder = str(end_year)
	else:
		# Try single year
		match_single = single_year_pattern.search(name_without_ext)
		if match_single:
			year_folder = match_single.group(1)

	# Create category folder based on the file name
	category_folder = os.path.join(base_dir, str(year_folder))
	os.makedirs(category_folder, exist_ok=True)
	file_path = os.path.join(category_folder, file_name)

	# Headers for the request
	headers = {
		'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
	}

	# Send GET request to fetch the file
	response = requests.get(url, headers=headers, verify=certifi.where())

	# Check if the request was successful
	if response.status_code == 200:
		# Save the content directly to an Excel file
		with open(file_path, "wb") as file:
			file.write(response.content)
		print("File downloaded successfully.")
	else:
		print(f"Failed to download the file. Status code: {response.status_code}")


for f in file_urls[:]:
	download(f)
	time.sleep(0.25)