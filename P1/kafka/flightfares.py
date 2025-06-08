import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
import time
import json
import os 
from path import GECKO_DRIVER_PATH, CITIES
from kafka_manager import KafkaManager


class FlightFareScraper():

    def __init__(self):
        self.GECKO_DRIVER_PATH = GECKO_DRIVER_PATH
        self.CITIES = CITIES
        self.kafka_topic = 'airfare'
        

    def flightscraper(self,source_city,destination_city,departure_date):
        
        driver_path = self.GECKO_DRIVER_PATH
        service = Service(driver_path)
        firefox_options = Options()
        firefox_options.add_argument("--headless")
        driver = Firefox(service=service,options=firefox_options)
        flight_list = []

        try:
            driver.get('https://www.google.com/travel/flights')

            button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz/div/div/div/div[2]/div[1]/div[3]/div[1]/div[1]/form[1]/div/div/button/span[6]'))
            )
            button.click()

            # Change trip type
            change_trip_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[1]/div[1]/div/div/div/div[1]/div'))
            )
            change_trip_button.click()

            # Select one-way trip
            one_way_trip_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[1]/div[1]/div/div/div/div[2]/ul/li[2]'))
            )
            one_way_trip_button.click()

            # Origin input
            origin_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[1]/div[1]/div/div/div[1]/div/div/input'))
            )
            origin_button.click()

            org_text_input = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[1]/div[6]/div[2]/div[2]/div[1]/div/input'))
            )
            org_text_input.send_keys(source_city)
            time.sleep(3)
            select_button = driver.find_element(By.XPATH,'/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[1]/div[6]/div[3]/ul/li[1]/div[2]/div[1]')
            select_button.click()

            # Destination input
            destination_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[1]/div[4]/div/div/div[1]/div/div/input'))
            )
            destination_button.click()

            dest_text_field = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[1]/div[6]/div[2]/div[2]/div[1]/div/input'))
            )
            dest_text_field.send_keys(destination_city)

            time.sleep(3)
            dest_select = driver.find_element(By.XPATH,'/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[1]/div[6]/div[3]/ul/li[1]/div[2]/div[1]')
            dest_select.click()

            # # Date input
            dept_date_element = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[2]/div/div/div[1]/div/div/div[1]/div/div[1]/div/input'))
            )
            dept_date_element.click()

            dept_date_field = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[2]/div/div/div[2]/div/div[2]/div[1]/div[1]/div[1]/div/input'))
            )
            dept_date_field.clear()
            dept_date_field.send_keys(departure_date)

            done_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[1]/div/div[2]/div[2]/div/div/div[2]/div/div[3]/div[3]/div/button/span'))
            )
            done_button.click()

            # Search
            search_button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div[1]/div[2]/div/button/span[2]'))
            )
            search_button.click()

            # # Parse results
            time.sleep(10)
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            # Find all flight containers - adjust selector based on actual structure
            flight_containers = soup.find_all('div', class_='OgQvJf')  # Use appropriate container class

            for container in flight_containers:
            # Extract details from individual container
                departure_span = container.find('span', {'aria-label': lambda x: x and 'Departure time' in x})
                departure_time = departure_span.text.strip() if departure_span else None

                arrival_span = container.find('span', {'aria-label': lambda x: x and 'Arrival time' in x})
                arrival_time = arrival_span.text.strip() if arrival_span else None

                duration_div = container.find('div', class_='gvkrdb')
                duration = duration_div.text.strip() if duration_div else None

                airline_div = container.find('div', class_='sSHqwe tPgKwe ogfYpf')
                airline = airline_div.find('span').text.strip() if airline_div else None

                price_span = container.find('span', {'data-gs': True})
                price = price_span.text.strip() if price_span else None

                flight_list.append({
                    'departure_time': departure_time,
                    'arrival_time': arrival_time,
                    'duration': duration,
                    'airline': airline,
                    'price': price
                })

        except Exception as e:
            print(f"Error occurred: {str(e)}")
        finally:
            driver.quit()
        
        return flight_list
    

    def get_all_fares(self,departure_date='1 May 2025'):

        kafka_manager = KafkaManager()
        try:
            for source_city in self.CITIES:
                for dest_city in self.CITIES: 
                    if source_city != dest_city:
                        flight_details = self.flightscraper(source_city=source_city,\
                                                            destination_city=dest_city,\
                                                            departure_date=departure_date)
                        
                        message = {
                            "type": "airfare",
                            "departure_city" : source_city,
                            "arrival_city": dest_city,
                            "departure_date": departure_date,
                            "data":flight_details
                        }

                        kafka_manager.scrape_and_send(topic=self.kafka_topic,data=message)
        finally:
            kafka_manager.kafka_producer.close()



if __name__ == "__main__":

    fs = FlightFareScraper()
    fs.get_all_fares()
                    

