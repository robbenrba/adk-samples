# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Google Maps Place Details tool for POI insights and reviews."""

import os
from google.adk.tools import ToolContext


def get_places_details(place_id: str, tool_context: ToolContext) -> dict:
    """Retrieve deep insights, reviews, and details for a specific place.

    This tool uses the Google Maps Place Details API to fetch granular data
    about a Point of Interest (POI), including user reviews, operating hours,
    website, and amenities.

    Args:
        place_id: The unique Google Maps Place ID (usually obtained from a
                  search_places tool result).
                  Example: "ChIJN1t_tDeuEmsRUsoyG83frY4"

    Returns:
        dict: A dictionary containing:
            - status: "success" or "error"
            - insights: Detailed dictionary of the place (reviews, hours, etc.)
            - error_message: Error details if status is "error"
    """
    try:
        import googlemaps

        # Get API key from session state first, then fall back to environment variable
        maps_api_key = tool_context.state.get("maps_api_key", "") or os.environ.get("MAPS_API_KEY", "")

        if not maps_api_key:
            return {
                "status": "error",
                "error_message": "Maps API key not found. Set MAPS_API_KEY environment variable or 'maps_api_key' in session state.",
                "insights": {},
            }

        # Initialize Google Maps client
        gmaps = googlemaps.Client(key=maps_api_key)

        # Define specific fields to fetch to manage costs and relevance.
        # fetching 'reviews' and 'editorial_summary' is key for "insights".
        fields = [
            'name',
            'formatted_address',
            'formatted_phone_number',
            'website',
            'rating',
            'user_ratings_total',
            'reviews',            # Key for sentiment analysis
            'opening_hours',      # Operational insights
            'editorial_summary',  # Google's own summary of the place
            'price_level',
            'types',
            'wheelchair_accessible_entrance', # Accessibility insight
            'serves_vegetarian_food',         # Dietary insight
            'delivery',
            'dine_in'
        ]

        # Perform Place Details lookup
        # Note: Some fields might not exist for all locations, the API handles this gracefully.
        place_details = gmaps.place(place_id=place_id, fields=fields)

        result = place_details.get('result', {})

        if not result:
            return {
                "status": "error",
                "error_message": "No details found for the provided Place ID.",
                "insights": {}
            }

        # Format Reviews for easier consumption by the Agent
        formatted_reviews = []
        for review in result.get('reviews', [])[:5]: # Limit to top 5 most relevant
            formatted_reviews.append({
                "author": review.get("author_name", "Anonymous"),
                "rating": review.get("rating", 0),
                "text": review.get("text", ""),
                "time": review.get("relative_time_description", "")
            })

        # Format Opening Hours into a readable list
        hours = result.get('opening_hours', {}).get('weekday_text', [])

        # Construct final insight object
        insights = {
            "name": result.get("name"),
            "summary": result.get("editorial_summary", {}).get("overview", "No summary available."),
            "contact": {
                "phone": result.get("formatted_phone_number", "N/A"),
                "website": result.get("website", "N/A"),
                "address": result.get("formatted_address")
            },
            "metrics": {
                "rating": result.get("rating"),
                "total_reviews": result.get("user_ratings_total"),
                "price_level": result.get("price_level", "N/A")
            },
            "amenities": {
                "wheelchair_accessible": result.get("wheelchair_accessible_entrance"),
                "vegetarian_options": result.get("serves_vegetarian_food"),
                "delivery": result.get("delivery"),
                "dine_in": result.get("dine_in")
            },
            "hours": hours,
            "latest_reviews": formatted_reviews
        }

        return {
            "status": "success",
            "insights": insights
        }

    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e),
            "insights": {},
        }