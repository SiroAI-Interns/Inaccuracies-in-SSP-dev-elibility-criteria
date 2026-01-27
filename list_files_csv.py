import os
import csv
from pathlib import Path

# Set the folder path
folder_path = r"e:\SIRO\Hungary_Files"
output_file = r"e:\SIRO\hungary_files_list.csv"

# Get all files (including subdirectories)
all_files = []

# Use os.walk for fast traversal
for root, dirs, files in os.walk(folder_path):
    for file in files:
        full_path = os.path.join(root, file)
        # Get relative path from Hungary_Files folder
        relative_path = os.path.relpath(full_path, folder_path)
        all_files.append(relative_path)

# Sort the files
all_files.sort()

# Write to CSV
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['File Names'])  # Header
    for file_name in all_files:
        writer.writerow([file_name])

print(f"✓ Total files found: {len(all_files)}")
print(f"✓ Output saved to: {output_file}")
