import random
from datetime import datetime, timedelta

def getStaticTransportOptions(from_city, to_city):
    """Get static transport options between cities"""
    # Define static transport options for each type
    transport_options = {
        'air': [
            {
                'provider': 'Iberia',
                'price': 150,
                'duration': '2h 30m',
                'departure_time': '10:00',
                'arrival_time': '12:30',
                'type': 'flight',
                'image': 'https://images.unsplash.com/photo-1436491865332-7a61a109cc05?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'In-flight Entertainment', 'Meal Included']
            },
            {
                'provider': 'Air France',
                'price': 180,
                'duration': '2h 45m',
                'departure_time': '14:30',
                'arrival_time': '17:15',
                'type': 'flight',
                'image': 'https://images.unsplash.com/photo-1436491865332-7a61a109cc05?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'In-flight Entertainment', 'Meal Included']
            }
        ],
        'train': [
            {
                'provider': 'Renfe',
                'price': 45,
                'duration': '4h 15m',
                'departure_time': '08:00',
                'arrival_time': '12:15',
                'type': 'train',
                'image': 'https://images.unsplash.com/photo-1544620347-c4fd4a3d5957?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Power Outlets', 'Food Service']
            },
            {
                'provider': 'SNCF',
                'price': 55,
                'duration': '4h 30m',
                'departure_time': '13:30',
                'arrival_time': '18:00',
                'type': 'train',
                'image': 'https://images.unsplash.com/photo-1544620347-c4fd4a3d5957?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Power Outlets', 'Food Service']
            }
        ],
        'bus': [
            {
                'provider': 'FlixBus',
                'price': 25,
                'duration': '6h 30m',
                'departure_time': '07:00',
                'arrival_time': '13:30',
                'type': 'bus',
                'image': 'https://images.unsplash.com/photo-1544620347-c4fd4a3d5957?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Power Outlets', 'Toilet']
            },
            {
                'provider': 'ALSA',
                'price': 30,
                'duration': '6h 45m',
                'departure_time': '15:00',
                'arrival_time': '21:45',
                'type': 'bus',
                'image': 'https://images.unsplash.com/photo-1544620347-c4fd4a3d5957?w=500&h=300&fit=crop',
                'amenities': ['Free WiFi', 'Power Outlets', 'Toilet']
            }
        ]
    }
    
    return transport_options 