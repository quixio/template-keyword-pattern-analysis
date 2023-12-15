from datetime import datetime, timedelta
import json

def sum_keywords_tumbling(row: dict, state: State, some_param):
    state_key = "counts_tumbling_v200"  # State key variable

    # Initialize state if it doesn't exist
    counts = state.get(state_key, {})

    ended_windows = {}  # Store ended windows

    # Get current timestamp
    current_timestamp = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    # Update counts
    for keyword, _ in row.items():
        if keyword != 'Timestamp':
            print(f"Processing keyword: {keyword}")  # Debug print
            for window_length in ["1min", "15min", "60min", "4hr", "8hr", "24hr"]:
                # Calculate window start time
                if window_length == "1min":
                    window_start = current_timestamp - timedelta(minutes=1)
                elif window_length == "15min":
                    window_start = current_timestamp - timedelta(minutes=15)
                elif window_length == "60min":
                    window_start = current_timestamp - timedelta(hours=1)
                elif window_length == "4hr":
                    window_start = current_timestamp - timedelta(hours=4)
                elif window_length == "8hr":
                    window_start = current_timestamp - timedelta(hours=8)
                elif window_length == "24hr":
                    window_start = current_timestamp - timedelta(hours=24)

                window_start_str = str(window_start.timestamp())

                if window_start_str not in counts:
                    counts[window_start_str] = {}

                window_counts = counts[window_start_str]

                # Reset counts at the end of each window
                if keyword in window_counts and datetime.fromtimestamp(float(max(window_counts[keyword].keys()))) < window_start:
                    print(f"End of window starting at {window_start_str} for keyword {keyword}: {window_counts[keyword]}")  # Debug print
                    if window_start_str not in ended_windows:
                        ended_windows[window_start_str] = {}
                    ended_windows[window_start_str][keyword] = sum(window_counts[keyword].values())
                    window_counts[keyword] = {}

                # Add new count
                if keyword not in window_counts:
                    window_counts[keyword] = {}
                if str(current_timestamp.timestamp()) not in window_counts[keyword]:
                    window_counts[keyword][str(current_timestamp.timestamp())] = 0
                window_counts[keyword][str(current_timestamp.timestamp())] += 1

                print(f"Updated counts for keyword {keyword} in window starting at {window_start_str}: {window_counts[keyword]}")  # Debug print

    # Debug print
    print({window_start: {keyword: sum(times.values()) for keyword, times in counts[window_start].items()} for window_start in counts}) 

    state.set(state_key, counts)
    return json.dumps(ended_windows)  # Return ended windows as JSON