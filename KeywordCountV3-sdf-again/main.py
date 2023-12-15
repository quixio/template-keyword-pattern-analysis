from datetime import datetime, timedelta
import json

def sum_keywords_tumbling(row: dict, state: State, some_param):
    state_key = "counts_tumbling_v2"  # State key variable

    # Initialize state if it doesn't exist
    counts = state.get(state_key, {
        "1min": {},
        "15min": {},
        "60min": {},
        "4hr": {},
        "8hr": {},
        "24hr": {}
    })

    ended_windows = {}  # Store ended windows

    # Get current timestamp
    current_timestamp = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    # Update counts
    for keyword, _ in row.items():
        if keyword != 'Timestamp':
            print(f"Processing keyword: {keyword}")  # Debug print
            for window in counts.keys():
                window_counts = counts[window]
                # Calculate window start time
                if window == "1min":
                    window_start = current_timestamp - timedelta(minutes=1)
                elif window == "15min":
                    window_start = current_timestamp - timedelta(minutes=15)
                elif window == "60min":
                    window_start = current_timestamp - timedelta(hours=1)
                elif window == "4hr":
                    window_start = current_timestamp - timedelta(hours=4)
                elif window == "8hr":
                    window_start = current_timestamp - timedelta(hours=8)
                elif window == "24hr":
                    window_start = current_timestamp - timedelta(hours=24)

                # Reset counts at the end of each window
                if keyword in window_counts and datetime.fromtimestamp(float(max(window_counts[keyword].keys()))) < window_start:
                    print(f"End of {window} window for keyword {keyword}: {window_counts[keyword]}")  # Debug print
                    if window not in ended_windows:
                        ended_windows[window] = {}
                    ended_windows[window][str(window_start.timestamp())] = sum(window_counts[keyword].values())
                    window_counts[keyword] = {}

                # Add new count
                if keyword not in window_counts:
                    window_counts[keyword] = {}
                if str(current_timestamp.timestamp()) not in window_counts[keyword]:
                    window_counts[keyword][str(current_timestamp.timestamp())] = 0
                window_counts[keyword][str(current_timestamp.timestamp())] += 1

                print(f"Updated counts for keyword {keyword} in window {window}: {window_counts[keyword]}")  # Debug print

    # Debug print
    print({window: {keyword: sum(times.values()) for keyword, times in counts[window].items()} for window in counts}) 

    state.set(state_key, counts)
    return json.dumps(ended_windows)  # Return ended windows as JSON