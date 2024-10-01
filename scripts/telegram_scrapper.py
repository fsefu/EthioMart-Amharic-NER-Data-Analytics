import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import csv
import time

class TelegramScraper:
    def __init__(self, base_url, media_dir='../data/media', start_id=3, end_id=5383):
        self.base_url = base_url  # Base URL without message ID
        self.media_dir = media_dir
        self.start_id = start_id
        self.end_id = end_id
        self.session = requests.Session()

        # Create media directory if it doesn't exist
        if not os.path.exists(self.media_dir):
            os.makedirs(self.media_dir)

    def fetch_page_content(self, url):
        """Fetch the HTML content of the page."""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return None

    def parse_html(self, html_content):
        """Parse HTML content to extract the required data."""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Ensure the page contains the required elements; skip if not found
        user_element = soup.find('div', class_='tgme_widget_message_user')
        author_element = soup.find('div', class_='tgme_widget_message_author')
        message_element = soup.find('div', class_='tgme_widget_message')

        if not user_element or not author_element or not message_element:
            print("Required elements not found, skipping this message.")
            return None  # Skip this message

        # Extract Channel Username and Title
        channel_username = user_element.find('a')['href'].split('/')[-1]
        channel_title = author_element.find('a').text.strip()

        # Extract Message ID
        message_id = message_element['data-post-id']

        # Extract Message Content
        message = soup.find('div', class_='tgme_widget_message_text')
        if message:
            message = message.text.strip()
        else:
            message = "No message content"  # Handle missing message content
        # Extract Date 
        date_element = soup.find('time')  # Assuming 'time' tag is where datetime is stored

        if date_element:
            # Safely try to get 'datetime', or use fallback
            date = date_element.get('datetime', None)
            if date is None:
                # Check for an edited date as fallback if 'datetime' is not available
                edited_date_element = soup.find('div', class_='edited_at_class')  # Replace 'edited_at_class' with actual class name
                if edited_date_element:
                    date = edited_date_element.get('datetime', 'Unknown Date')  # Use a default/fallback value
                else:
                    date = 'Unknown Date'
        else:
            date = 'Unknown Date'

        # Format the date only if it's not the fallback 'Unknown Date'
        try:
            formatted_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S+00:00').strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            formatted_date = 'Unknown Date'  # Use 'Unknown Date' if the format doesn't match or parsing fails

        print(f"Formatted Date: {formatted_date}")


        # # Extract Date
        # date_element = soup.find('time')  # Assuming 'time' tag is where datetime is stored
        
        # if date_element:
        #     # Safely try to get 'datetime', or use fallback
        #     date = date_element.get('datetime', None)
        #     if date is None:
        #         # Check for an edited date as fallback if 'datetime' is not available
        #         edited_date_element = soup.find('div', class_='edited_at_class')  # Replace 'edited_at_class' with actual class name
        #         if edited_date_element:
        #             date = edited_date_element.get('datetime', 'Unknown Date')  # Use a default/fallback value
        #         else:
        #             date = 'Unknown Date'
        # else:
        #     date = 'Unknown Date'
        # # date_element = soup.find('time')
        # # date = date_element['datetime']
        # formatted_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S+00:00').strftime('%Y-%m-%d %H:%M:%S')

        # Extract Media Path (if available)
        media_element = soup.find('a', class_='tgme_widget_message_photo_wrap')
        media_url = None
        if media_element:
            media_url = media_element['style'].split("url('")[1].split("')")[0]

        return {
            'channel_title': channel_title,
            'channel_username': channel_username,
            'message_id': message_id,
            'message': message,
            'date': formatted_date,
            'media_url': media_url
        }

    def download_media(self, media_url, channel_username, message_id):
        """Download media and save it in the media folder."""
        if media_url:
            try:
                media_response = self.session.get(media_url)
                media_response.raise_for_status()

                # Define file path
                media_extension = media_url.split('.')[-1]
                media_filename = f'{channel_username}_{message_id}.{media_extension}'
                media_path = os.path.join(self.media_dir, media_filename)

                # Save the media file
                with open(media_path, 'wb') as media_file:
                    media_file.write(media_response.content)
                
                return media_path
            except requests.exceptions.RequestException as e:
                print(f"Error downloading media: {e}")
                return None
        return None

    def save_data_json(self, data, filename='../data/scraped_data.json'):
        """Save the scraped data to a JSON file."""
        with open(filename, 'w') as file:
            json.dump(data, file, indent=4)

    def save_data_csv(self, data, filename='../data/scraped_data.csv'):
        """Save the scraped data to a CSV file."""
        headers = ['channel_title', 'channel_username', 'message_id', 'message', 'date', 'media_url', 'media_path']

        file_exists = os.path.isfile(filename)
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            if not file_exists:
                writer.writeheader()  # Write headers only if the file is new

            writer.writerow(data)

    def scrape(self):
        """Main function to scrape messages from a range of IDs and save to CSV."""
        for message_id in range(self.start_id, self.end_id + 1):
            url = f"{self.base_url}/{message_id}?embed=1&mode=tme"
            print(f"Scraping message ID {message_id}...")

            html_content = self.fetch_page_content(url)
            if html_content:
                parsed_data = self.parse_html(html_content)

                if parsed_data:
                    # Download the media if available
                    if parsed_data['media_url']:
                        media_path = self.download_media(parsed_data['media_url'], parsed_data['channel_username'], parsed_data['message_id'])
                        parsed_data['media_path'] = media_path
                    else:
                        parsed_data['media_path'] = None

                    # Save the scraped data to CSV
                    self.save_data_csv(parsed_data)
                    
            # Introduce a small delay between requests to avoid overloading the server
            time.sleep(1)  # Sleep for 1 second between requests (adjustable)

if __name__ == "__main__":
    # Base URL of the Telegram channel (without message ID)
    base_url = "https://t.me/Shageronlinestore"
    
    # Initialize scraper with a range of message IDs
    scraper = TelegramScraper(base_url=base_url, start_id=3, end_id=5383)
    
    # Start scraping
    scraper.scrape()

    print("Scraping completed.")
