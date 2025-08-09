import streamlit as st
import requests
import json
import time
import os
from datetime import datetime
from bs4 import BeautifulSoup

def remove_html_tags(text):
    "Use BeautifulSoup to remove HTML tags from a string"
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text()

def process_data(data):
    "Modify data, removing HTML from 'TRESC_INTERESARIUSZ' if present"
    for dictionary in data['dokument']['fields']:
        if dictionary['key'] == 'TRESC_INTERESARIUSZ':
            dictionary['value'] = remove_html_tags(dictionary['value'])
    return data

def save_error_logs(start, end, not_found_errors, forbidden_errors, other_errors):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    error_filename = f'error_logs_{start}_{end}_{timestamp}.txt'
    with open(os.path.join('error_logs', error_filename), 'w') as f:
        f.write("Not Found Errors:\n")
        for url in not_found_errors:
            f.write(url + '\n')

        f.write("\nForbidden Errors:\n")
        for url in forbidden_errors:
            f.write(url + '\n')

        f.write("\nOther Errors:\n")
        for url in other_errors:
            f.write(url + '\n')

def scrape_data(start, end, batch_size):
    base_url = 'https://eureka.mf.gov.pl/api/public/v1/informacje/'
    dir_name = 'produkcja'
    os.makedirs(dir_name, exist_ok=True)
    os.makedirs('error_logs', exist_ok=True)  # create error_logs folder if it doesn't exist
    
    accumulated_data = []
    number_saved = 0
    batch_start = start

    not_found_errors = []
    forbidden_errors = []
    other_errors = []

    for i in range(start, end+1):
        url = base_url + str(i)
        print(f'Processing: {url}')  # this line will log the current URL being processed
        response = requests.get(url)
        data = response.json()

        if 'errors' in data:
            error_code = data['errors'][0]['errorCode']
            if error_code == 'NOT_FOUND':
                print(f'Error for ID: {i}, skipping...')
                not_found_errors.append(url)
                continue
            elif error_code == 'FORBIDDEN':
                print(f'Forbidden access for ID: {i}, saving error...')
                forbidden_errors.append(url)
                continue
            else:
                other_errors.append(url)
                continue
        else:
            data = process_data(data)  # process the data before adding it to accumulated data
            accumulated_data.append(data)
            number_saved += 1
            
            if len(accumulated_data) >= batch_size:
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                filename = f'{batch_start}_{i}_{timestamp}.json'
                with open(os.path.join(dir_name, filename), 'w', encoding='utf-8') as f:
                    json.dump(accumulated_data, f, ensure_ascii=False, indent=4)
                print(f'Successfully saved data as {filename}')
                save_error_logs(batch_start, i, not_found_errors, forbidden_errors, other_errors)  # Save error logs after each batch
                accumulated_data = []
                not_found_errors = []
                forbidden_errors = []
                other_errors = []
                batch_start = i + 1

        time.sleep(1)
    
    if accumulated_data:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'{batch_start}_{i}_{timestamp}.json'
        with open(os.path.join(dir_name, filename), 'w', encoding='utf-8') as f:
            json.dump(accumulated_data, f, ensure_ascii=False, indent=4)
        print(f'Successfully saved data as {filename}')
        
    if not_found_errors or forbidden_errors or other_errors:
        save_error_logs(batch_start, end, not_found_errors, forbidden_errors, other_errors)  # Save remaining error logs

        

# Set the title of the app
st.title('Eureka Web Scraping App')

# Set up inputs for the start and end numbers
start_number = st.number_input('Enter a start number', value=555295)
end_number = st.number_input('Enter an end number', value=555555)

# Set up input for the batch size with specific options
batch_size_options = [1, 10, 25, 50, 100, 250, 500, 1000]
batch_size = st.selectbox('Select batch size', batch_size_options, index=batch_size_options.index(1000))

# Set up a button to start the web scraping when clicked
if st.button('Start Scraping'):
    print('Starting web scraping...')
    scrape_data(start_number, end_number, batch_size)
    print('Web scraping completed.')
