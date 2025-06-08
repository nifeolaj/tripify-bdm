from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from datetime import datetime
from kafka_manager import KafkaManager


class AlsaBusScraper:
    def __init__(self, headless=True):
        self.base_url = "https://www.alsa.com/en/home"
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

    def _accept_cookies(self):
        try:
            WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
            ).click()
        except Exception:
            pass  

    def _input_station(self, field_id, station_name):
        field = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.ID, f"_{field_id}_")))
        field.click()
        
        input_field = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//input[contains(@id, '{field_id}')]")))
        input_field.clear()
        time.sleep(1)
        input_field.send_keys(station_name)
        time.sleep(3)
        input_field.send_keys(Keys.DOWN, Keys.RETURN)
        time.sleep(1)

    def _input_date(self, date):
        # Input Travel Date
        date_button = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(@id, 'departureDate_bt')]")))
        date_button.click()

        date_input = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'departureDate_bt')]")))
        
        date_input.clear()
        date_input.send_keys(date.strftime("%m/%d/%Y"))
        time.sleep(1)

    def search_trips(self, origin, destination, date):
        # Search for bus trips between origin and destination on given date
        print(f"Searching for bus trips from {origin} to {destination} on {date.strftime('%m/%d/%Y')}")
        
        try:
            self.driver.get(self.base_url)
            self._accept_cookies()
            
            # Input stations and date
            self._input_station("originStationNameId", origin)
            self._input_station("destinationStationNameId", destination)
            self._input_date(date)
            
            # Click search button
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "journeySearchFormButtonjs"))).click()
            
            # Wait for results
            time.sleep(10)
            return self._extract_trip_data()
            
        except Exception as e:
            print(f"Error during search: {e}")
            raise

    def _extract_trip_data(self):
        # Extract trip data from search results page
        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        trip_elements = soup.select('[id^="itemParrillaOutward"]')
        print(f"Found {len(trip_elements)} trips")
        
        trips = []
        for trip in trip_elements:
            try:
                trips.append(self._parse_trip_element(trip))
            except Exception as e:
                print(f"Error processing trip: {e}")
                continue
        
        print(f"Successfully extracted data for {len(trips)} trips")
        return trips

    def _parse_trip_element(self, trip_element):
        price_text = trip_element.select_one("span.bottom.btn.btn-primary.ng-binding.ng-scope").text
        price = price_text.replace('€', '').strip().replace(' ', '').replace(',', '.')
        
        return {
            "Departure": trip_element.select_one("p.destinos > span.resaltado.left.icn-arrow-right.ng-binding").text.strip(),
            "Arrival": trip_element.select_one("p.destinos > span.right.ng-binding").text.strip(),
            "Departure Time": trip_element.select_one("span.hora.hora-salida.left.ng-binding").text.strip(),
            "Arrival Time": trip_element.select_one("span.hora.hora-llegada.right.ng-binding").text.strip(),
            "Duration": trip_element.select_one("span.time-travel.time.ng-binding").text.strip(),
            "Price": price
        }

    def save_to_json(self, trips, filename):
        if not trips:
            print("No data to save")
            return None
            
        pd.DataFrame(trips).to_json(filename, orient="records", indent=4)
        return filename
    
    def close(self):
        self.driver.quit()


def search_bus_routes(origin, destination, month, day, max_retries=3):
    for attempt in range(max_retries):
        scraper = None
        try:
            scraper = AlsaBusScraper()
            search_date = datetime(year=2025, month=month, day=day)
            trips = scraper.search_trips(origin, destination, search_date)
            
            if trips:
                # filename = f"{origin.strip().lower()}_{destination.strip().lower()}_{search_date.strftime('%d%m%Y')}_bus_trips.json"
                # scraper.save_to_json(trips, filename)
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
    origins = ["MADRID", "BARCELONA", "PARIS "]
    destinations = ["MADRID", "BARCELONA", "PARIS "]
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
                        trips_df = search_bus_routes(origin, destination, 5, day)
                        
                        message = {
                                "type": "busfare",
                                "departure_city" : origin,
                                "arrival_city": destination,
                                "departure_date": date_str,
                                "data":trips_df.to_dict('records') if not trips_df.empty else []
                            }
                        
                        # all_results[route_key][date_str] = trips_df.to_dict('records') if not trips_df.empty else []
                        kafka_manager.scrape_and_send(topic="busfare",data=message)

                    except Exception as e:
                        print(f"Failed to scrape {origin} to {destination} for {date_str}: {str(e)}")
        
    finally:
            kafka_manager.kafka_producer.close()       

    
    return 

if __name__ == "__main__":
    run_test_scenarios()