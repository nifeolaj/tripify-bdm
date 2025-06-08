import pandas as pd
import numpy as np
from datetime import datetime, timezone
from google.cloud import storage

import re
import io


def clean_text(text):
    """
    Text cleaning with enhanced encoding fixes for French characters
    """
    if pd.isna(text):
        return text
    
    text = str(text)
    
    # Handle specific cases
    special_replacements = {'ÃƒÂ©': 'é', 'ÃƒÂ¨': 'è', 'ÃƒÂª': 'ê', 'ÃƒÂ«': 'ë', 'ÃƒÂ¢': 'â', 'ÃƒÂ®': 'î',
                            'ÃƒÂ´': 'ô', 'ÃƒÂ»': 'û', 'ÃƒÂ¹': 'ù', 'ÃƒÂ§': 'ç', 'ÃƒÂ€': 'À', 'ÃƒÂ‰': 'É',
                            'ÃƒÂˆ': 'È', 'ÃƒÂŠ': 'Ê', 'ÃƒÂ‹': 'Ë', 'ÃƒÂ‚': 'Â', 'ÃƒÂŽ': 'Î', 'ÃƒÂ"': 'Ô',
                            'ÃƒÂ›': 'Û', 'ÃƒÂ™': 'Ù', 'ÃƒÂ‡': 'Ç', 'ÂÂ«': '"', 'ÂÂ»': '"', 'âÂ€Â™': "'", 
                            'âÂ€Âœ': '"', 'âÂ€Â': '"', 'âÂ€"': '-', 'âÂ€˜': "'", 'Ã©': 'é', 'Ã¨': 'è',  
                            'Ãª': 'ê', 'Ã«': 'ë', 'Ã¢': 'â', 'Ã®': 'î', 'Ã´': 'ô', 'Ã»': 'û', 'Ã¹': 'ù', 
                            'Ã§': 'ç', 'Ã€': 'À', 'Ã‰': 'É', 'Ãˆ': 'È', 'ÃŠ': 'Ê', 'Ã‹': 'Ë', 'Ã‚': 'Â',
                            'ÃŽ': 'Î', 'Ã"': 'Ô', 'Ã›': 'Û', 'Ã™': 'Ù', 'Ã‡': 'Ç', 'â€™': "'", 'â€œ': '"', 
                            'â€': '"', '\u2018': "'", '\u2019': "'", '\u201c': '"', '\u201d': '"', 'Ã¯Â»Â¿': ''}
    
    # Apply replacements in order of longest first to prevent partial replacements
    for wrong, right in sorted(special_replacements.items(), key=lambda x: -len(x[0])):
        text = text.replace(wrong, right)
    
    # Handle HTML tags 
    text = re.sub(r'<[^>]+>', '', text)
    

    # Clean up any remaining odd characters
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)  # Remove control characters

    replacements = [
        (r'ÂÂ\s*!Ã°ÂŸÂšÂ²', '!'),     
        (r'âÂœÂ', ''),                
        (r'ÃƒÂ', 'à'),                 
        (r'ÂÂ\s*!Ã°ÂŸÂ\'Â¯', '!'),    
        (r'ÂÂ', ''),                  
        (r'âÂ€Â™', "'")               
    ]
    
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)
    
    # Normalize whitespace and quotes
    text = re.sub(r'[\u201c\u201d]', '"', text)  
    text = re.sub(r'[\u2018\u2019]', "'", text)  
    
    # Clean whitespace and return
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_paris_events(input_file):
    """
    Cleaning for Paris events data with consistent structure
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file, encoding="utf-8")
    
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
    
    
    # Text fields with cleaning
    cleaned_df['id'] = df['Event ID']
    cleaned_df['name'] = df['Titre'].apply(clean_text)
    cleaned_df['description'] = df['Chapeau'].apply(clean_text)
    cleaned_df['activity'] = df['Description'].apply(clean_text)
    
    # Convert is_free to 'free'/'paid' 
    cleaned_df['is_free'] = np.where(df['Type de prix'].str.lower() == 'gratuit', 'free', 'paid')
    
    cleaned_df['price_details'] = df['DÃ©tail du prix'].apply(clean_text).str.replace('â¬', '€')
    
    # Convert dates to ISO format with timezone
    def convert_date(date_str):
        if pd.isna(date_str) or str(date_str).strip() == '':
            return np.nan
        
        try:
            dt = pd.to_datetime(date_str)
            return dt.isoformat()  
        except:
            return np.nan
    
    cleaned_df['start_date'] = df['Date de dÃ©but'].apply(convert_date)
    cleaned_df['end_date'] = df['Date de fin'].apply(convert_date)
    
    # Extract time from occurrences if available
    def extract_time(occurrences):
        if pd.isna(occurrences):
            return np.nan
        try:
            first_time = occurrences.split('_')[0].split('T')[1][:5]  # Get HH:MM
            return first_time
        except:
            return np.nan
    
    cleaned_df['time'] = df['Occurrences'].apply(extract_time)
    cleaned_df['excluded_days'] = np.nan  
    
    cleaned_df['website'] = df['URL'].apply(clean_text)
    cleaned_df['contact_phone'] = df['TÃ©lÃ©phone de contact'].apply(clean_text)
    cleaned_df['contact_email'] = df['Email de contact'].apply(clean_text)
    cleaned_df['venue'] = df['Nom du lieu'].apply(clean_text)
    cleaned_df['address'] = df['Adresse du lieu'].apply(clean_text)
    
    # Split coordinates into latitude and longitude
    def split_coords(coord_str):
        if pd.isna(coord_str):
            return (np.nan, np.nan)
        try:
            lat, lon = map(float, coord_str.split(','))
            return (lat, lon)
        except:
            return (np.nan, np.nan)
    
    cleaned_df['latitude'], cleaned_df['longitude'] = zip(*df['CoordonnÃ©es gÃ©ographiques'].apply(split_coords))
    cleaned_df['x_coordinate'] = np.nan 
    cleaned_df['y_coordinate'] = np.nan 
    
    # Address components
    cleaned_df['zip_code'] = df['Code postal'].apply(clean_text)
    cleaned_df['city'] = df['Ville'].apply(clean_text)
    cleaned_df['country'] = 'France'
    cleaned_df['address_neighbourhood'] = np.nan  
    cleaned_df['address_district'] = np.nan 
    
    # Category and audience
    cleaned_df['category'] = df['qfap_tags'].apply(clean_text)
    cleaned_df['audience'] = df['audience'].apply(clean_text)
    cleaned_df['organisation'] = df['contact_organisation_name'].apply(clean_text)
    
    # Social media handles
    cleaned_df['linkedin_handle'] = df['URL LinkedIn associÃ©e'].apply(clean_text)
    cleaned_df['instagram_handle'] = df['URL Instagram associÃ©e'].apply(clean_text)
    cleaned_df['facebook_handle'] = df['URL Facebook associÃ©e'].apply(clean_text)
    cleaned_df['twitter_handle'] = df['URL Twitter associÃ©e'].apply(clean_text)
    
    # Capitalize appropriate fields
    for col in ['name', 'venue', 'city']:
        cleaned_df[col] = cleaned_df[col].str.title()
    
    # Remove duplicates 
    cleaned_df = cleaned_df.drop_duplicates()
    
    
    # Ensure column order
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
