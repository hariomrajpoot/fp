# Detailed Comments for generic-ingestion.py

# Import necessary libraries
import pandas as pd
import numpy as np

# Define a function to read data from a source
# This function takes a file path as input and returns a DataFrame

def read_data(file_path):
    """Reads data from a specified file path and returns a DataFrame."""
    data = pd.read_csv(file_path)  # Read the data using pandas
    return data

# Define a function to process the data
# This function takes a DataFrame as input and performs cleaning operations

def process_data(df):
    """Cleans the input DataFrame by removing null values and duplicates."""
    df_cleaned = df.dropna()  # Remove rows containing NaN
    df_cleaned = df_cleaned.drop_duplicates()  # Remove duplicate rows
    return df_cleaned

# Define a function to save processed data back to a file
# This function takes a DataFrame and an output path as input

def save_data(df, output_path):
    """Saves the cleaned DataFrame to the specified output file path."""
    df.to_csv(output_path, index=False)  # Save DataFrame to CSV without index

# Main execution flow
if __name__ == '__main__':
    # Specify input and output file paths
    input_file = 'data/raw_data.csv'
    output_file = 'data/cleaned_data.csv'

    # Read the data
    raw_data = read_data(input_file)  # Calling read_data function
    
    # Process the data
    cleaned_data = process_data(raw_data)  # Calling process_data function
    
    # Save the cleaned data
    save_data(cleaned_data, output_file)  # Calling save_data function
