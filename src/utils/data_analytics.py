import re
from collections import defaultdict
import pandas as pd

def _clean_and_split_quals(text: str) -> list:
    """
    (Helper function) Cleans a text block and splits it into individual qualifications.
    It removes boilerplate text and formats each qualification string.
    """
    if not isinstance(text, str):
        return []

    # Remove boilerplate text like 'Amazon is an equal opportunities employer...'
    boilerplate_pattern = r'Amazon is an equal opportunities employer.*'
    text = re.sub(boilerplate_pattern, '', text, flags=re.DOTALL)
    
    # Split the text by newline, and clean up each line
    quals = [re.sub(r'^- ', '', line).strip() for line in text.split('\n') if line.strip()]
    return [qual for qual in quals if qual]


def get_skills_by_category(df: pd.DataFrame, job_category: str) -> list:
    """
    Extracts and counts all basic and preferred qualifications for a given job category.

    Args:
        df (pd.DataFrame): The input DataFrame containing job listings.
        job_category (str): The specific job category to filter by.

    Returns:
        list: A sorted list of tuples, where each tuple contains the qualification 
              string, a dictionary of its counts, and the total count.
              The list is sorted in descending order of total count.
    """
    # If job_category is 'ALL', use the entire DataFrame. Otherwise, filter by category.
    if job_category.upper() == 'ALL':
        filtered_df = df.copy()
    else:
        filtered_df = df[df['job_category'] == job_category].copy()

    if filtered_df.empty:
        print(f"No jobs found for category: '{job_category}'.")
        return []

    # Dictionary to store qualifications and their counts
    qualifications = defaultdict(lambda: {'basic_count': 0, 'preferred_count': 0})

    # Iterate through the filtered DataFrame and populate the dictionary
    for _, row in filtered_df.iterrows():
        # Process basic qualifications
        basic_quals = _clean_and_split_quals(str(row.get('basic_qual', '')))
        for qual in basic_quals:
            qualifications[qual]['basic_count'] += 1
            
        # Process preferred qualifications
        pref_quals = _clean_and_split_quals(str(row.get('pref_qual', '')))
        for qual in pref_quals:
            qualifications[qual]['preferred_count'] += 1
    
    # Create a list of tuples with total counts
    sorted_quals = []
    for qual, counts in qualifications.items():
        total_count = counts['basic_count'] + counts['preferred_count']
        sorted_quals.append((qual, counts, total_count))

    # Sort the list by total count in descending order
    sorted_quals.sort(key=lambda x: x[2], reverse=True)
    
    return sorted_quals