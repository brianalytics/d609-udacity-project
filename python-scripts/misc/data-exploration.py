###############################################################################################################
### Prior to starting the project, I run the code below to check the counts of the files that I downloaded. ###
###############################################################################################################

import os
import json

def count_json_rows(folder_path):
    total = 0
    for root, _, files in os.walk(folder_path):  # Handling nested folders
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    try:
                        # Case 1: Line-delimited JSON
                        line_count = sum(1 for _ in f)
                        total += line_count
                    except json.JSONDecodeError:
                        # Case 2: JSON array format
                        f.seek(0)  # Reset the file pointer
                        data = json.load(f)
                        total += len(data)
    return total

# Assigning and calling the function with the file paths specified
customer_count = count_json_rows('data/customer/landing')
accelerometer_count = count_json_rows('data/accelerometer/landing')
step_trainer_count = count_json_rows('data/step_trainer/landing')

# Output resulkts and expected results
print(f"Customer records: {customer_count} (Expected: 956)")
print(f"Accelerometer records: {accelerometer_count} (Expected: 81,273)")
print(f"Step Trainer records: {step_trainer_count} (Expected: 28,680)")
