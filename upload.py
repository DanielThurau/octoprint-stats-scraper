#!/usr/bin/env python3

from dotenv import load_dotenv
import fcntl
import gspread
import json
import os


def lock_file(f):
    fcntl.lockf(f, fcntl.LOCK_EX)


def unlock_file(f):
    fcntl.lockf(f, fcntl.LOCK_UN)


def auth_and_get_sheet(spreadsheet_key):
    gc = gspread.service_account()
    sh = gc.open_by_key(spreadsheet_key)
    return sh.sheet1


def extract_events_from_file(filename):
    # Read the JSON file
    with open(filename, 'r') as file:
        data = json.load(file)

    # If the file is empty, no work to do
    if len(data) == 0:
        return []

    print_events = []
    # Iterate over the events
    for event_id, print_event in data["events"].items():
        if print_event["event_type"] == "PRINT_DONE":
            print_events.append(print_event["data"])
    return print_events


# Extract the useful data from an event
def process_event(data):
    row = [data["file"], data["ptime"], data["bed_actual"], data["tool0_actual"], data["tool0_length"],
           data["event_time"]]
    return row


def append_row(sheet, row):
    sheet.append_row(row)


def clear_file(filename):
    try:
        with open(filename, 'w+') as file:
            lock_file(file)  # Attempt to lock the file

            # Clear the file if it has been processed
            file.write("{}")

            unlock_file(file)  # Unlock the file
    except IOError as e:
        print(f"Could not process the file due to I/O error: {e}")


if __name__ == "__main__":
    # Load secrets
    load_dotenv()
    SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')
    SOURCE_FILE = os.getenv('SOURCE_FILE')

    # Authenticate and get the Google sheet
    gsheet = auth_and_get_sheet(SPREADSHEET_KEY)

    # Process the events and upload them to the Google sheet
    events = extract_events_from_file(SOURCE_FILE)
    for event in events:
        append_row(gsheet, process_event(event))

    # After processing all the events, clear the source file to attempt a
    # non-perfect deduplication effort. Print's have a unique UUID so
    # better deduplication can be done afterwards as well.
    clear_file(SOURCE_FILE)
