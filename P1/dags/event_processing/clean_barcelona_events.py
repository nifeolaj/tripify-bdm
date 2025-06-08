import pandas as pd
import numpy as np
import io
from datetime import datetime

def clean_barcelona_events(input_file):
    """
    Clean and transform Barcelona events data
    
    Args:
        input_file (str): Path to the input file
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file, sep='\t', encoding='utf-16', escapechar='\\')
    
    # Initialize new dataframe
    cleaned_df = pd.DataFrame(columns=[
        'id', 'name', 'description', 'activity', 'is_free', 'price_details',
        'start_date', 'end_date', 'time', 'excluded_days', 'website',
        'contact_phone', 'contact_email', 'venue', 'address',
        'address_neighbourhood', 'address_district', 'zip_code', 'city', 'country',
        'x_coordinate', 'y_coordinate', 'latitude', 'longitude', 'category',
        'audience', 'organisation', 'linkedin_handle', 'instagram_handle',
        'facebook_handle', 'twitter_handle'
    ])
    
   
    # Direct mappings from source data
    cleaned_df['id'] = df['register_id']
    cleaned_df['name'] = df['name']
    cleaned_df['description'] = df['values_description']
    cleaned_df['start_date'] = df['start_date']
    cleaned_df['end_date'] = df['end_date']
    cleaned_df['contact_phone'] = df['values_value']
    cleaned_df['address_neighbourhood'] = df['addresses_neighborhood_name']
    cleaned_df['address_district'] = df['addresses_district_name']
    cleaned_df['zip_code'] = df['addresses_zip_code']
    cleaned_df['city'] = df['addresses_town']
    cleaned_df['x_coordinate'] = df['geo_epgs_25831_x']
    cleaned_df['y_coordinate'] = df['geo_epgs_25831_y']
    cleaned_df['latitude'] = df['geo_epgs_4326_lat']
    cleaned_df['longitude'] = df['geo_epgs_4326_lon']
    cleaned_df['organisation'] = df['institution_name']
    
    # Set constant value for country
    cleaned_df['country'] = 'Spain'
    
    # Create combined address
    def format_address(row):
        road = str(row['addresses_road_name']) if pd.notna(row['addresses_road_name']) else ''
        start_num = str(int(row['addresses_start_street_number'])) if pd.notna(row['addresses_start_street_number']) else ''
        end_num = str(int(row['addresses_end_street_number'])) if pd.notna(row['addresses_end_street_number']) else ''
        
        address = f"{road} {start_num}"
        if end_num and end_num != start_num:
            address += f"-{end_num}"
        return address.strip()
    
    cleaned_df['address'] = df.apply(format_address, axis=1)
    
    
    # Columns not present in source data that we need to create as NaN
    missing_columns = [
        'activity', 'is_free', 'price_details', 'time', 'excluded_days',
        'website', 'contact_email', 'venue', 'category', 'audience', 'linkedin_handle',
        'instagram_handle', 'facebook_handle', 'twitter_handle'
    ]
    
    for col in missing_columns:
        cleaned_df[col] = np.nan
    
    
    # Convert dates to datetime
    for col in ['start_date', 'end_date']:
        cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
    
    # Clean text fields
    text_cols = ['name', 'description', 'address_neighbourhood', 'address_district', 
                'city', 'organisation', 'address']
    
    for col in text_cols:
        cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
        cleaned_df[col] = cleaned_df[col].str.replace(r'\s+', ' ', regex=True)
        
        # Capitalize name fields
        if col in ['name', 'city', 'organisation']:
            cleaned_df[col] = cleaned_df[col].str.title()

    # Clean phone numbers
    cleaned_df['contact_phone'] = cleaned_df['contact_phone'].astype(str).str.strip()
    cleaned_df['contact_phone'] = cleaned_df['contact_phone'].str.replace(r'[^\d+]', '', regex=True)
    
    # Convert numeric coordinates
    for col in ['x_coordinate', 'y_coordinate', 'latitude', 'longitude']:
        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')

    # Remove duplicates
    cleaned_df = cleaned_df.drop_duplicates()
    
    
    # Ensure columns are in the same order 
    final_columns = [
        'id', 'name', 'description', 'activity', 'is_free', 'price_details',
        'start_date', 'end_date', 'time', 'excluded_days', 'website',
        'contact_phone', 'contact_email', 'venue', 'address',
        'address_neighbourhood', 'address_district', 'zip_code', 'city', 'country',
        'x_coordinate', 'y_coordinate', 'latitude', 'longitude', 'category',
        'audience', 'organisation', 'linkedin_handle', 'instagram_handle',
        'facebook_handle', 'twitter_handle'
    ]
    
    cleaned_df = cleaned_df[final_columns]
    
    return cleaned_df