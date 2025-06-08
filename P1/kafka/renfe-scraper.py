from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import re
from datetime import datetime
from kafka_manager import KafkaManager

class RenfeScraper:
    MONTHS_MAP = {
        "Enero": 1, "Febrero": 2, "Marzo": 3, "Abril": 4,
        "Mayo": 5, "Junio": 6, "Julio": 7, "Agosto": 8,
        "Septiembre": 9, "Octubre": 10, "Noviembre": 11, "Diciembre": 12
    }
    
    def __init__(self, headless=True):
        self.base_url = "https://www.renfe.com/"
        self.driver = self._init_driver(headless)
        
    def _init_driver(self, headless):
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--start-maximized")
        
        if headless:
            options.add_argument("--headless")
            
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )

    def _parse_month_name_to_number(self, month_name):
        return self.MONTHS_MAP.get(month_name.capitalize())

    def _extract_date_components(self, date):
        return date.year, date.month, date.day

    def _input_station(self, field_id, station_name):
        field = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, field_id)))
        field.click()
        
        input_field = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//input[@id='{field_id}']")))
        input_field.clear()
        time.sleep(2)
        input_field.send_keys(station_name)
        time.sleep(3)
        input_field.send_keys(Keys.DOWN, Keys.RETURN)
        time.sleep(1)

    def _navigate_to_correct_month(self, target_year, target_month):
        # Navigate calendar to the target date
        while True:
            current_month_year = self.driver.find_element(
                By.XPATH, "//span[@class='rf-daterange-picker-alternative__month-label']").text.strip()
            current_month_name, current_year = current_month_year[:-4], int(current_month_year[-4:])
            current_month = self._parse_month_name_to_number(current_month_name)

            if current_year == target_year and current_month == target_month:
                break

            # Navigate to correct month/year
            if (current_year < target_year) or (current_year == target_year and current_month < target_month):
                self.driver.find_element(
                    By.XPATH, "//button[@class='lightpick__next-action']").click()
            else:
                self.driver.find_element(
                    By.XPATH, "//button[@class='lightpick__previous-action']").click()
            time.sleep(1)

    def setup_search(self, origin, destination, date):
        # Set up the search parameters on Renfe website
        target_year, target_month, target_day = self._extract_date_components(date)
        
        try:
            self.driver.get(self.base_url)
            time.sleep(5)

            # Input stations
            self._input_station("origin", origin)
            self._input_station("destination", destination)

            # Set date
            try:
                date_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@id='first-input']")))
                date_button.click()
            except Exception:
                date_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@class='rf-daterange-picker-alternative__ipt']")))
                date_button.click()

            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//label[@for='trip-go']"))).click()
            time.sleep(2)

            
            self._navigate_to_correct_month(target_year, target_month)

            # Select day
            day_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, 
                    f"//div[contains(@class, 'lightpick__day') and contains(@class, 'is-available') and text()='{target_day}']")))
            day_button.click()
            time.sleep(2)

            # Open passenger selection and search
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[@id='passengersSelection']"))).click()
            time.sleep(4)

            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, 
                    "//button[@type='submit' and contains(@class, 'rf-button--primary')]"))).click()
            time.sleep(10)

            return self.extract_trip_data(origin, destination)

        except Exception as e:
            print(f"Error in search setup: {e}")
            return []

    def extract_trip_data(self, origin, destination):
        # Extract trip data from search results
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        trip_elements = soup.select('div.row.selectedTren')
        print(f"Found {len(trip_elements)} trips")
        
        trips = []
        for trip in trip_elements:
            try:
                times = trip.select('h5', attrs={'aria-hidden': 'true'})
                price_match = re.search(r'(\d+,\d+)', trip.select_one("span.precio-final").text.strip())
                
                trips.append({
                    "Departure": origin,
                    "Arrival": destination,
                    "Departure Time": times[0].text.strip(),
                    "Arrival Time": times[1].text.strip(),
                    "Duration": trip.select_one("span.text-number").text.strip(),
                    "Price": price_match.group(1) if price_match else "N/A"
                })
            except Exception as e:
                print(f"Error processing trip: {e}")
                continue
                
        print(f"Successfully extracted data for {len(trips)} trips")
        return trips

    def save_to_json(self, trips, origin, destination, date):
        """Save trip data to JSON file"""
        if not trips:
            print("No data to save")
            return None
            
        filename = f"{origin.strip().lower()}_{destination.strip().lower()}_{date.strftime('%d%m%Y')}_trains_renfe.json"
        pd.DataFrame(trips).to_json(filename, orient="records", indent=4)
        return filename

    def close(self):
        self.driver.quit()

def search_renfe_routes(origin, destination, year, month, day, max_retries=3):
    for attempt in range(max_retries):
        scraper = None
        try:
            scraper = RenfeScraper()
            search_date = datetime(year=year, month=month, day=day)
            trips = scraper.setup_search(origin, destination, search_date)
            
            if trips:
                # scraper.save_to_json(trips, origin, destination, search_date)
                return pd.DataFrame(trips)
            return pd.DataFrame()
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 2
                print(f"Attempt {attempt + 1} failed. Error: {e}")
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All retry attempts failed. Final error: {e}")
                raise
        finally:
            if scraper:
                scraper.close()
    
    return pd.DataFrame()

############################## TEST ##############################

def run_test_scenarios():
    origins = ["MADRID", "BARCELONA"]
    destinations = ["MADRID", "BARCELONA"]
    # all_results = {}
    kafka_manager = KafkaManager()

    try:
        for origin in origins:
            for destination in destinations:
                if origin == destination:
                    continue

                # route_key = f"{origin.lower()}_{destination.lower()}"
                # all_results[route_key] = {}
                
                for day in range(1, 15):
                    date_str = datetime(2025, 5, day).strftime("%d%m%Y")
                    try:
                        trips_df = search_renfe_routes(origin, destination, 2025, 5, day)
                        # all_results[route_key][date_str] = trips_df.to_dict('records')
                        message = {
                                    "type": "trainfare",
                                    "departure_city" : origin,
                                    "arrival_city": destination,
                                    "departure_date": date_str,
                                    "data":trips_df.to_dict('records') if not trips_df.empty else []
                                }
                        kafka_manager.scrape_and_send(topic="trainfare",data=message)
                    except Exception as e:
                        print(f"Failed to scrape {origin} to {destination} for {date_str}: {str(e)}")
    finally:
            kafka_manager.kafka_producer.close() 
    
    return 

if __name__ == "__main__":
    run_test_scenarios()