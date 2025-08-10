import streamlit as st
import requests
import json
import time
import os
from datetime import datetime

# HTML content is preserved as-is; no tag removal

def process_data(data):
    "Return data without modifications (preserve all fields, including HTML)"
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

def scrape_data(start, end, batch_size, per_item_delay_seconds, batch_delay_seconds):
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

    total = end - start + 1
    progress_bar = st.progress(0.0)
    status_text = st.empty()

    for i in range(start, end+1):
        url = base_url + str(i)
        status_text.write(f"Processing: {url}")
        print(f'Processing: {url}')  # this line will log the current URL being processed
        response = requests.get(url)
        data = response.json()

        if 'errors' in data:
            error_code = data['errors'][0]['errorCode']
            if error_code == 'NOT_FOUND':
                msg = f'[{i - start + 1}/{total}] NOT_FOUND for ID: {i}, skipping...'
                status_text.write(msg)
                print(msg)
                not_found_errors.append(url)
                progress_bar.progress((i - start + 1) / total)
                # Delay between individual items
                if per_item_delay_seconds and per_item_delay_seconds > 0:
                    time.sleep(per_item_delay_seconds)
                continue
            elif error_code == 'FORBIDDEN':
                msg = f'[{i - start + 1}/{total}] FORBIDDEN for ID: {i}, logging error...'
                status_text.write(msg)
                print(msg)
                forbidden_errors.append(url)
                progress_bar.progress((i - start + 1) / total)
                # Delay between individual items
                if per_item_delay_seconds and per_item_delay_seconds > 0:
                    time.sleep(per_item_delay_seconds)
                continue
            else:
                msg = f'[{i - start + 1}/{total}] Error for ID: {i}, logging error and skipping...'
                status_text.write(msg)
                print(msg)
                other_errors.append(url)
                progress_bar.progress((i - start + 1) / total)
                # Delay between individual items
                if per_item_delay_seconds and per_item_delay_seconds > 0:
                    time.sleep(per_item_delay_seconds)
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
                save_msg = f'Successfully saved data as {filename}'
                status_text.write(save_msg)
                print(save_msg)
                save_error_logs(batch_start, i, not_found_errors, forbidden_errors, other_errors)  # Save error logs after each batch
                accumulated_data = []
                not_found_errors = []
                forbidden_errors = []
                other_errors = []
                batch_start = i + 1
                # Delay between batches
                if batch_delay_seconds and batch_delay_seconds > 0:
                    time.sleep(batch_delay_seconds)

        # Delay between individual items
        if per_item_delay_seconds and per_item_delay_seconds > 0:
            time.sleep(per_item_delay_seconds)

        # Update progress bar after each item
        progress_bar.progress((i - start + 1) / total)
    
    if accumulated_data:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'{batch_start}_{i}_{timestamp}.json'
        with open(os.path.join(dir_name, filename), 'w', encoding='utf-8') as f:
            json.dump(accumulated_data, f, ensure_ascii=False, indent=4)
        final_save_msg = f'Successfully saved data as {filename}'
        status_text.write(final_save_msg)
        print(final_save_msg)
        
    if not_found_errors or forbidden_errors or other_errors:
        save_error_logs(batch_start, end, not_found_errors, forbidden_errors, other_errors)  # Save remaining error logs

    progress_bar.progress(1.0)
    st.success(f"Completed. Saved {number_saved} records. Errors: NOT_FOUND={len(not_found_errors)}, FORBIDDEN={len(forbidden_errors)}, OTHER={len(other_errors)}")

        

# Set the title of the app
st.title('Eureka Web Scraping App')

# Set up inputs for the start and end numbers
start_number = st.number_input('Enter a start number', value=555295)
end_number = st.number_input('Enter an end number', value=555300)

# Set up input for the batch size with specific options
batch_size_options = [1, 10, 25, 50, 100, 250, 500, 1000]
batch_size = st.selectbox('Select batch size', batch_size_options, index=batch_size_options.index(1000))

# Delay settings
per_item_delay_seconds = st.number_input('Delay between items (seconds)', min_value=0.0, value=0.30, step=0.1)
batch_delay_seconds = st.number_input('Delay between batches (seconds)', min_value=0, value=0, step=10)

# Set up a button to start the web scraping when clicked
if st.button('Start Scraping'):
    print('Starting web scraping...')
    scrape_data(start_number, end_number, batch_size, per_item_delay_seconds, batch_delay_seconds)
    print('Web scraping completed.')
