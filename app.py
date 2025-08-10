import streamlit as st
import requests
import json
import time
import os
from datetime import datetime
import aiohttp
import asyncio
import platform
import sys

# HTML content is preserved as-is; no tag removal

def process_data(data):
    "Return data without modifications (preserve all fields, including HTML)"
    return data

def save_error_logs(start, end, not_found_errors, forbidden_errors, other_errors):
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    error_filename = f'error_logs_{start}_{end}_{timestamp}.txt'
    os.makedirs('error_logs', exist_ok=True)
    with open(os.path.join('error_logs', error_filename), 'w') as f:
        f.write("Not Found Errors:\n")
        for url in not_found_errors:
            f.write(url + '\n')

        f.write("\nForbidden Errors:\n")
        for url in forbidden_errors:
            f.write(url + '\n')

        f.write("\nOther Errors:\n")
        for url, error in other_errors:
            f.write(f"{url}: {error}\n")

def print_cli_progress(message):
    """Print message to CLI"""
    print(message, flush=True)

# Synchronous version
def scrape_data(start, end, batch_size):
    base_url = 'https://eureka.mf.gov.pl/api/public/v1/informacje/'
    dir_name = 'produkcja'
    os.makedirs(dir_name, exist_ok=True)
    os.makedirs('error_logs', exist_ok=True)
    
    accumulated_data = []
    number_saved = 0
    batch_start = start

    not_found_errors = []
    forbidden_errors = []
    other_errors = []

    total = end - start + 1
    
    for i in range(start, end+1):
        progress = (i - start + 1) / total * 100
        url = base_url + str(i)
        status_msg = f'[{progress:.1f}%] Processing: {url}'
        st.write(status_msg)
        print_cli_progress(status_msg)
        
        response = requests.get(url)
        data = response.json()

        if 'errors' in data:
            error_code = data['errors'][0]['errorCode']
            if error_code == 'NOT_FOUND':
                error_msg = f'Error for ID: {i}, skipping...'
                st.write(error_msg)
                print_cli_progress(error_msg)
                not_found_errors.append(url)
                continue
            elif error_code == 'FORBIDDEN':
                error_msg = f'Forbidden access for ID: {i}, saving error...'
                st.write(error_msg)
                print_cli_progress(error_msg)
                forbidden_errors.append(url)
                continue
            else:
                other_errors.append((url, str(data['errors'])))
                continue
        else:
            data = process_data(data)
            accumulated_data.append(data)
            number_saved += 1
            
            if len(accumulated_data) >= batch_size:
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                filename = f'{batch_start}_{i}_{timestamp}.json'
                with open(os.path.join(dir_name, filename), 'w', encoding='utf-8') as f:
                    json.dump(accumulated_data, f, ensure_ascii=False, indent=4)
                save_msg = f'Successfully saved data as {filename}'
                st.write(save_msg)
                print_cli_progress(save_msg)
                save_error_logs(batch_start, i, not_found_errors, forbidden_errors, other_errors)
                accumulated_data = []
                not_found_errors = []
                forbidden_errors = []
                other_errors = []
                batch_start = i + 1

        time.sleep(1)
    
    if accumulated_data:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'{batch_start}_{end}_{timestamp}.json'
        with open(os.path.join(dir_name, filename), 'w', encoding='utf-8') as f:
            json.dump(accumulated_data, f, ensure_ascii=False, indent=4)
        save_msg = f'Successfully saved data as {filename}'
        st.write(save_msg)
        print_cli_progress(save_msg)
        
    if not_found_errors or forbidden_errors or other_errors:
        save_error_logs(batch_start, end, not_found_errors, forbidden_errors, other_errors)
        print_cli_progress(f'Saved error logs for {len(not_found_errors) + len(forbidden_errors) + len(other_errors)} errors')

    return number_saved

# Asynchronous version
async def fetch_data(session, url, i, progress_bar, rate_limit_delay, total, start):
    """Fetch data from a URL asynchronously"""
    try:
        progress_pct = ((i + 1 - start) / total) * 100
        status_msg = f'[{progress_pct:.1f}%] Processing: {url}'
        print_cli_progress(status_msg)
        
        async with session.get(url) as response:
            data = await response.json()
            progress_bar.progress((i + 1 - start) / total)
            await asyncio.sleep(rate_limit_delay)  # Respect rate limits
            return i, data, None
    except Exception as e:
        error_msg = f'Error for ID {i}: {str(e)}'
        print_cli_progress(error_msg)
        progress_bar.progress((i + 1 - start) / total)
        return i, None, str(e)

async def scrape_data_async(start, end, batch_size, concurrent_requests, rate_limit_delay, progress_placeholder):
    base_url = 'https://eureka.mf.gov.pl/api/public/v1/informacje/'
    dir_name = 'produkcja'
    os.makedirs(dir_name, exist_ok=True)
    os.makedirs('error_logs', exist_ok=True)
    
    total_requests = end - start + 1
    status_text = progress_placeholder.empty()
    progress_bar = st.progress(0.0)
    
    print_cli_progress(f"Starting async scraping of {total_requests} records with {concurrent_requests} concurrent requests")
    print_cli_progress(f"Batch size: {batch_size}, Rate limit delay: {rate_limit_delay}s")
    
    accumulated_data = []
    number_saved = 0
    current_batch_start = start

    not_found_errors = []
    forbidden_errors = []
    other_errors = []

    # Use semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(concurrent_requests)
    
    async def bounded_fetch(session, url, i):
        async with semaphore:
            return await fetch_data(session, url, i, progress_bar, rate_limit_delay, total_requests, start)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(start, end + 1):
            url = base_url + str(i)
            tasks.append(bounded_fetch(session, url, i))
        
        status_text.write(f"Processing {len(tasks)} requests...")
        print_cli_progress(f"Created {len(tasks)} tasks for async processing")
        
        # Process results as they complete
        for future in asyncio.as_completed(tasks):
            i, data, error = await future
            if error:
                error_msg = f"Error fetching {i}: {error}"
                other_errors.append((base_url + str(i), error))
                status_text.write(error_msg)
                continue
                
            if 'errors' in data:
                error_code = data['errors'][0]['errorCode']
                if error_code == 'NOT_FOUND':
                    msg = f'Error for ID: {i}, skipping... (NOT_FOUND)'
                    status_text.write(msg)
                    print_cli_progress(msg)
                    not_found_errors.append(base_url + str(i))
                    continue
                elif error_code == 'FORBIDDEN':
                    msg = f'Forbidden access for ID: {i}, saving error...'
                    status_text.write(msg)
                    print_cli_progress(msg)
                    forbidden_errors.append(base_url + str(i))
                    continue
                else:
                    other_errors.append((base_url + str(i), str(data['errors'])))
                    continue
            else:
                processed_data = process_data(data)
                accumulated_data.append((i, processed_data))  # Store with index for sorting later
                number_saved += 1
                
                # Show periodic status
                if number_saved % 10 == 0:
                    status_msg = f"Processed {number_saved}/{total_requests} records successfully"
                    status_text.write(status_msg)
                    print_cli_progress(status_msg)
        
        # Sort accumulated data by index before saving
        accumulated_data.sort(key=lambda x: x[0])
        # Remove indices after sorting
        sorted_data = [item[1] for item in accumulated_data]
        
        # Save in batches
        batch_count = 0
        for i in range(0, len(sorted_data), batch_size):
            batch = sorted_data[i:i+batch_size]
            batch_count += 1
            
            if batch:
                batch_start = start + i
                batch_end = min(batch_start + len(batch) - 1, end)
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                filename = f'{batch_start}_{batch_end}_{timestamp}.json'
                
                with open(os.path.join(dir_name, filename), 'w', encoding='utf-8') as f:
                    json.dump(batch, f, ensure_ascii=False, indent=4)
                
                save_msg = f'Successfully saved batch {batch_count} as {filename} ({len(batch)} records)'
                status_text.write(save_msg)
                print_cli_progress(save_msg)
    
    # Save error logs at the end
    if not_found_errors or forbidden_errors or other_errors:
        save_error_logs(start, end, not_found_errors, forbidden_errors, other_errors)
        error_msg = f'Saved error logs: {len(not_found_errors)} not found, {len(forbidden_errors)} forbidden, {len(other_errors)} other errors'
        status_text.write(error_msg)
        print_cli_progress(error_msg)

    progress_bar.progress(1.0)
    completion_msg = f"Web scraping completed. Saved {number_saved} records."
    status_text.write(completion_msg)
    print_cli_progress(completion_msg)
    return number_saved

def run_async_code(coro):
    """Helper to run async code with proper event loop setup"""
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# Streamlit UI
st.title('Eureka Web Scraping App')

st.sidebar.header("Scraping Settings")
start_number = st.sidebar.number_input('Start number', value=1, min_value=1)
end_number = st.sidebar.number_input('End number', value=555555, min_value=1)

# Validate that end number is greater than or equal to start number
if end_number < start_number:
    st.sidebar.error("End number must be greater than or equal to start number")
    end_number = start_number

# Calculate and display the number of records to scrape
num_records = end_number - start_number + 1
st.sidebar.info(f"Number of records to scrape: {num_records}")

# Batch size settings
batch_size_options = [1, 10, 25, 50, 100, 250, 500, 1000]
batch_size = st.sidebar.selectbox('Batch size', batch_size_options, index=batch_size_options.index(1000))

# Async settings
use_async = st.sidebar.checkbox("Use asynchronous scraping", value=True)

if use_async:
    st.sidebar.subheader("Async Settings")
    concurrent_requests = st.sidebar.slider("Concurrent requests", min_value=1, max_value=20, value=5, 
                                           help="Number of parallel requests. Higher values may improve speed but could get rate-limited.")
    rate_limit_delay = st.sidebar.slider("Rate limit delay (seconds)", min_value=0.0, max_value=5.0, value=0.5, step=0.1,
                                        help="Delay between requests to avoid rate limiting")

# Start scraping button
if st.button('Start Scraping'):
    progress_placeholder = st.empty()
    start_msg = 'Starting web scraping...'
    progress_placeholder.write(start_msg)
    print_cli_progress(start_msg)
    print_cli_progress(f"Range: {start_number} to {end_number} ({num_records} records)")
    
    start_time = time.time()
    
    if use_async:
        saved_count = run_async_code(scrape_data_async(
            start_number, end_number, batch_size, 
            concurrent_requests, rate_limit_delay, progress_placeholder
        ))
    else:
        saved_count = scrape_data(start_number, end_number, batch_size)
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    completion_msg = f"Web scraping completed in {elapsed:.2f} seconds. Saved {saved_count} records."
    st.success(completion_msg)
    print_cli_progress(completion_msg)
    
    # Calculate performance metrics
    records_per_second = saved_count / elapsed if elapsed > 0 else 0
    print_cli_progress(f"Performance: {records_per_second:.2f} records/second")