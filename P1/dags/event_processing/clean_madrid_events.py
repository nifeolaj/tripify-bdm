import pandas as pd
import numpy as np
from datetime import datetime, timezone
import re
import io


def clean_text(text):
    """
    Text cleaning with encoding fixes for texts with special characters
    """
    if pd.isna(text):
        return text
    
    text = str(text)
    
    # First fix the specific A°± issue and other common problems
    replacements = {
        'A°±': 'ñ',  
        'A±': 'ñ', '‰':'e',
        'Ã±': 'ñ',
        'Ã¡': 'á',
        'Ã©': 'é',
        'Ã': 'í',
        'Ã³': 'ó',
        'Ãº': 'ú',
        'Ã¼': 'ü',
        'Ã': 'Í',
        'Ã': 'Ó',
        'Ã': 'Ú',
        'Ã': 'Ñ',
        'Â': '',
        'â‚¬': '€',
        'â€"': '-',
        'â€œ': '"',
        'â€': '"',
        'â€˜': "'",
        'â€™': "'",
        '\u2018': "'",  # Left single quote
        '\u2019': "'",  # Right single quote
        '\u201c': '"',  # Left double quote
        '\u201d': '"',  # Right double quote
        '°±': 'ñ'  # Additional fix for the malformed ñ
    }
    
    for wrong, right in replacements.items():
        text = text.replace(wrong, right)
    
    # Handle any remaining encoding issues
    try:
        # Try UTF-8 first
        text = text.encode('utf-8', errors='replace').decode('utf-8')
    except:
        try:
            # Fallback to latin1 if UTF-8 fails
            text = text.encode('latin1', errors='replace').decode('utf-8', errors='replace')
        except:
            # Final fallback to just clean what we can
            pass
    
    # Clean whitespace and return
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_madrid_events(input_file):
    """
    Improved cleaning for Madrid events data
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
    
    
    # Text fields with enhanced cleaning
    cleaned_df['id'] = df[' ID-EVENTO']
    cleaned_df['name'] = df['TITULO'].apply(clean_text)
    cleaned_df['description'] = df['DESCRIPCION'].apply(clean_text)
    cleaned_df['activity'] = df['TITULO-ACTIVIDAD'].apply(clean_text)
    
    # Convert is_free to 'free'/'paid' 
    cleaned_df['is_free'] = np.where(df['GRATUITO'] == 1, 'free', 'paid')
    
    cleaned_df['price_details'] = df['PRECIO'].apply(clean_text)
    
    # Convert dates to ISO format with timezone
    def convert_date(date_str):
        try:
            dt = pd.to_datetime(date_str)
            return dt.isoformat() + '+01:00'  
        except:
            return np.nan
    
    cleaned_df['start_date'] = df['FECHA'].apply(convert_date)
    cleaned_df['end_date'] = df['FECHA-FIN'].apply(convert_date)
    
    cleaned_df['time'] = df['HORA'].apply(clean_text)
    cleaned_df['excluded_days'] = df['DIAS-EXCLUIDOS'].apply(clean_text)
    cleaned_df['website'] = df['CONTENT-URL'].apply(clean_text)
    cleaned_df['venue'] = df['NOMBRE-INSTALACION'].apply(clean_text)
    cleaned_df['address_neighbourhood'] = df['BARRIO-INSTALACION'].apply(clean_text)
    cleaned_df['address_district'] = df['DISTRITO-INSTALACION'].apply(clean_text)
    cleaned_df['zip_code'] = df['CODIGO-POSTAL-INSTALACION'].apply(clean_text)
    
    # Coordinates
    cleaned_df['x_coordinate'] = pd.to_numeric(df['COORDENADA-X'], errors='coerce')
    cleaned_df['y_coordinate'] = pd.to_numeric(df['COORDENADA-Y'], errors='coerce')
    cleaned_df['latitude'] = pd.to_numeric(df['LATITUD'], errors='coerce')
    cleaned_df['longitude'] = pd.to_numeric(df['LONGITUD'], errors='coerce')
    
    # Constants
    cleaned_df['city'] = 'Madrid'
    cleaned_df['country'] = 'Spain'
    
    # Create combined address with proper formatting
    def format_address(row):
        road_type = clean_text(row['CLASE-VIAL-INSTALACION']) if pd.notna(row['CLASE-VIAL-INSTALACION']) else ''
        road_name = clean_text(row['NOMBRE-VIA-INSTALACION']) if pd.notna(row['NOMBRE-VIA-INSTALACION']) else ''
        number = str(int(row['NUM-INSTALACION'])) if pd.notna(row['NUM-INSTALACION']) else ''
        
        address_parts = []
        if road_type:
            address_parts.append(road_type)
        if road_name:
            address_parts.append(road_name)
        if number:
            address_parts.append(number)
        
        return ' '.join(address_parts).strip()
    
    cleaned_df['address'] = df.apply(format_address, axis=1)

    # Category and audience with better cleaning
    def extract_last_part(text):
        if pd.isna(text):
            return np.nan
        parts = str(text).split('/')
        return clean_text(parts[-1].strip())
    
    cleaned_df['category'] = df['TIPO'].apply(extract_last_part)
    # Split into separate words (if camelCase or PascalCase)
    cleaned_df['category'] = cleaned_df['category'].str.replace(
        r'([a-z])([A-Z])',  # Finds lowercase followed by uppercase
        r'\1 \2',           # Adds a space between them
        regex=True
    )
    cleaned_df['audience'] = df['AUDIENCIA'].apply(extract_last_part)
    
    # Missing columns
    missing_cols = ['contact_phone', 'contact_email', 'organisation',
                   'linkedin_handle', 'instagram_handle', 'facebook_handle', 'twitter_handle']
    for col in missing_cols:
        cleaned_df[col] = np.nan
    
    # Capitalize appropriate fields
    for col in ['name', 'venue', 'address_neighbourhood', 'address_district']:
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
