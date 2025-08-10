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

def _safe_rerun():
    rerun_fn = getattr(st, 'rerun', None)
    if rerun_fn is None:
        st.experimental_rerun()
    else:
        rerun_fn()


def initialize_run(start: int, end: int, batch_size: int, per_item_delay_seconds: float, batch_delay_seconds: int) -> None:
    """Initialize session state for a cooperative, stoppable scraping run."""
    ss = st.session_state
    ss.is_running = True
    ss.stop_requested = False

    ss.start_number = int(start)
    ss.end_number = int(end)
    ss.current_id = int(start)
    ss.total = int(end) - int(start) + 1

    ss.batch_size = int(batch_size)
    ss.per_item_delay_seconds = float(per_item_delay_seconds)
    ss.batch_delay_seconds = int(batch_delay_seconds)

    ss.base_url = 'https://eureka.mf.gov.pl/api/public/v1/informacje/'
    ss.dir_name = 'produkcja'
    os.makedirs(ss.dir_name, exist_ok=True)
    os.makedirs('error_logs', exist_ok=True)

    ss.accumulated_data = []
    ss.number_saved = 0
    ss.batch_start = int(start)

    ss.not_found_errors = []
    ss.forbidden_errors = []
    ss.other_errors = []

    ss.total_not_found_count = 0
    ss.total_forbidden_count = 0
    ss.total_other_count = 0

    ss.last_status_message = ''
    ss.last_processed_id = None


def finalize_run() -> None:
    """Finalize the run: save any remaining data and error logs, reset running flags, and report."""
    ss = st.session_state
    last_processed_id = (ss.current_id - 1) if ss.current_id > ss.start_number else ss.start_number

    if ss.accumulated_data:
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'{ss.batch_start}_{last_processed_id}_{timestamp}.json'
        with open(os.path.join(ss.dir_name, filename), 'w', encoding='utf-8') as f:
            json.dump(ss.accumulated_data, f, ensure_ascii=False, indent=4)
        msg = f'Successfully saved data as {filename}'
        ss.last_status_message = msg
        print(msg)

    if ss.not_found_errors or ss.forbidden_errors or ss.other_errors:
        save_error_logs(ss.batch_start, last_processed_id, ss.not_found_errors, ss.forbidden_errors, ss.other_errors)

    ss.is_running = False
    # Do not clear stop flag so we can show "Stopped" feedback; we'll clear it on next start
    st.success(
        f"Completed. Saved {ss.number_saved} records. Errors: NOT_FOUND={ss.total_not_found_count}, "
        f"FORBIDDEN={ss.total_forbidden_count}, OTHER={ss.total_other_count}"
    )


def process_next_item() -> None:
    """Process a single item based on the current session state."""
    ss = st.session_state
    i = ss.current_id
    if i > ss.end_number:
        return

    url = ss.base_url + str(i)
    ss.last_status_message = f"Processing: {url}"
    print(f'Processing: {url}')

    try:
        response = requests.get(url)
        data = response.json()
    except Exception as exc:  # network or parsing error -> treat as OTHER
        msg = f"[{(i - ss.start_number + 1)}/{ss.total}] Exception for ID: {i}: {exc}. Logging and skipping..."
        ss.last_status_message = msg
        print(msg)
        ss.other_errors.append(url)
        ss.total_other_count += 1
        # per-item delay
        if ss.per_item_delay_seconds and ss.per_item_delay_seconds > 0:
            time.sleep(ss.per_item_delay_seconds)
        ss.current_id += 1
        return

    if 'errors' in data:
        error_code = data['errors'][0].get('errorCode')
        if error_code == 'NOT_FOUND':
            msg = f"[{(i - ss.start_number + 1)}/{ss.total}] NOT_FOUND for ID: {i}, skipping..."
            ss.last_status_message = msg
            print(msg)
            ss.not_found_errors.append(url)
            ss.total_not_found_count += 1
        elif error_code == 'FORBIDDEN':
            msg = f"[{(i - ss.start_number + 1)}/{ss.total}] FORBIDDEN for ID: {i}, logging error..."
            ss.last_status_message = msg
            print(msg)
            ss.forbidden_errors.append(url)
            ss.total_forbidden_count += 1
        else:
            msg = f"[{(i - ss.start_number + 1)}/{ss.total}] Error for ID: {i}, logging error and skipping..."
            ss.last_status_message = msg
            print(msg)
            ss.other_errors.append(url)
            ss.total_other_count += 1
    else:
        data = process_data(data)
        ss.accumulated_data.append(data)
        ss.number_saved += 1

        if len(ss.accumulated_data) >= ss.batch_size:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            filename = f'{ss.batch_start}_{i}_{timestamp}.json'
            with open(os.path.join(ss.dir_name, filename), 'w', encoding='utf-8') as f:
                json.dump(ss.accumulated_data, f, ensure_ascii=False, indent=4)
            save_msg = f'Successfully saved data as {filename}'
            ss.last_status_message = save_msg
            print(save_msg)
            save_error_logs(ss.batch_start, i, ss.not_found_errors, ss.forbidden_errors, ss.other_errors)
            ss.accumulated_data = []
            ss.not_found_errors = []
            ss.forbidden_errors = []
            ss.other_errors = []
            ss.batch_start = i + 1
            if ss.batch_delay_seconds and ss.batch_delay_seconds > 0:
                time.sleep(ss.batch_delay_seconds)

    # per-item delay
    if ss.per_item_delay_seconds and ss.per_item_delay_seconds > 0:
        time.sleep(ss.per_item_delay_seconds)

    ss.last_processed_id = i
    ss.current_id += 1

        

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

# Initialize session-state defaults
if 'is_running' not in st.session_state:
    st.session_state.is_running = False
if 'stop_requested' not in st.session_state:
    st.session_state.stop_requested = False

# Progress and status UI
progress_value = 0.0
if st.session_state.get('is_running'):
    total = max(1, int(st.session_state.get('total', 1)))
    progressed = int(st.session_state.get('current_id', 0)) - int(st.session_state.get('start_number', 0))
    progress_value = min(1.0, max(0.0, progressed / total))
progress_bar = st.progress(progress_value)
# Small counter near the progress bar showing processed/total
progress_counter = st.empty()
if st.session_state.get('is_running'):
    processed_count = max(0, int(st.session_state.get('current_id', 0)) - int(st.session_state.get('start_number', 0)))
    total_count = max(0, int(st.session_state.get('total', 0)))
    progress_counter.caption(f"{processed_count}/{total_count}")
else:
    progress_counter.caption("")
status_text = st.empty()
status_text.write(st.session_state.get('last_status_message', ''))

# Single Start/Stop toggle button
toggle_label = 'Stop' if st.session_state.get('is_running') else 'Start'
if st.button(toggle_label, key='run_toggle_button'):
    if st.session_state.get('is_running'):
        st.session_state.stop_requested = True
    else:
        print('Starting web scraping...')
        initialize_run(start_number, end_number, batch_size, per_item_delay_seconds, batch_delay_seconds)
    _safe_rerun()

# Engine: process items cooperatively without threads
if st.session_state.get('is_running'):
    # If stop requested or finished, finalize
    if st.session_state.get('stop_requested') or st.session_state.get('current_id', 0) > st.session_state.get('end_number', 0):
        finalize_run()
    else:
        process_next_item()
        _safe_rerun()
