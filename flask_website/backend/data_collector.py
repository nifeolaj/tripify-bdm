import os
from datetime import datetime
import csv
import json
import uuid
from google.cloud import storage
import logging

logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self):
        self.storage_client = storage.Client()
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        self.bucket = self.storage_client.bucket(self.bucket_name)
        
        # Initialize data buffers
        self.likes_buffer = []
        self.posts_buffer = []
        self.users_buffer = []
        
        # Set buffer size limit
        self.buffer_limit = 1000
        
        # Initialize last flush time
        self.last_flush_time = None

    def _get_today_path(self):
        """Get today's path in GCS bucket"""
        today = datetime.now().strftime('%Y-%m-%d')
        return f'users/{today}'

    def _write_to_csv(self, data, filename):
        """Write data to a CSV file"""
        if not data:
            return None

        # Create temporary file
        temp_path = f'/tmp/{filename}'
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)

        # Write to CSV
        with open(temp_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        return temp_path

    def _upload_to_gcs(self, local_path, gcs_path):
        """Upload file to GCS"""
        try:
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            os.remove(local_path)  # Clean up local file
            return True
        except Exception as e:
            logger.error(f"Error uploading to GCS: {e}")
            return False

    def collect_like(self, user_id, post_id):
        """Collect like data"""
        like_data = {
            'like_id': str(uuid.uuid4()),
            'user_id': user_id,
            'post_id': post_id,
            'created_at': datetime.now().isoformat()
        }
        self.likes_buffer.append(like_data)
        
        if len(self.likes_buffer) >= self.buffer_limit:
            self.flush_likes()

    def collect_post(self, post_data):
        """Collect post data"""
        self.posts_buffer.append(post_data)
        
        if len(self.posts_buffer) >= self.buffer_limit:
            self.flush_posts()

    def collect_user(self, user_data):
        """Collect user data"""
        self.users_buffer.append(user_data)
        
        if len(self.users_buffer) >= self.buffer_limit:
            self.flush_users()

    def _update_flush_time(self):
        """Update the last flush time"""
        self.last_flush_time = datetime.now()

    def flush_all(self):
        """Flush all buffers to GCS"""
        logger.info("Starting scheduled daily flush of all data")
        try:
            self.flush_likes()
            self.flush_posts()
            self.flush_users()
            self._update_flush_time()
            logger.info("Successfully completed daily flush")
        except Exception as e:
            logger.error(f"Error during daily flush: {e}")
            raise

    def flush_likes(self):
        """Flush likes buffer to GCS"""
        if not self.likes_buffer:
            logger.info("No likes data to flush")
            return

        logger.info(f"Flushing {len(self.likes_buffer)} likes records")
        temp_path = self._write_to_csv(self.likes_buffer, 'likes.csv')
        if temp_path:
            gcs_path = f"{self._get_today_path()}/likes.csv"
            if self._upload_to_gcs(temp_path, gcs_path):
                logger.info(f"Successfully uploaded likes data to {gcs_path}")
                self.likes_buffer = []
                self._update_flush_time()

    def flush_posts(self):
        """Flush posts buffer to GCS"""
        if not self.posts_buffer:
            logger.info("No posts data to flush")
            return

        logger.info(f"Flushing {len(self.posts_buffer)} posts records")
        temp_path = self._write_to_csv(self.posts_buffer, 'posts.csv')
        if temp_path:
            gcs_path = f"{self._get_today_path()}/posts.csv"
            if self._upload_to_gcs(temp_path, gcs_path):
                logger.info(f"Successfully uploaded posts data to {gcs_path}")
                self.posts_buffer = []
                self._update_flush_time()

    def flush_users(self):
        """Flush users buffer to GCS"""
        if not self.users_buffer:
            logger.info("No users data to flush")
            return

        logger.info(f"Flushing {len(self.users_buffer)} users records")
        temp_path = self._write_to_csv(self.users_buffer, 'users.csv')
        if temp_path:
            gcs_path = f"{self._get_today_path()}/users.csv"
            if self._upload_to_gcs(temp_path, gcs_path):
                logger.info(f"Successfully uploaded users data to {gcs_path}")
                self.users_buffer = []
                self._update_flush_time() 