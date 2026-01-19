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

"""Advanced Analytics tool for Places Insights BigQuery Dataset."""

import os
from google.cloud import bigquery
from google.adk.tools import ToolContext

def analyze_market_gaps(
    city: str,
    business_type: str,
    analysis_mode: str,
    target_amenity: str = None,
    tool_context: ToolContext = None
) -> dict:
    """
    Performs advanced market analysis using Places Insights Core Schema.

    Args:
        city: Target city (e.g., "Chicago").
        business_type: The primary type (e.g., "coffee_shop", "gym").
        analysis_mode: The type of analysis to run. Options:
            - "AMENITY_GAP": Finds % of places lacking a specific feature.
            - "VULNERABILITY": Finds areas with high traffic but low ratings.
            - "PRICE_DISTRIBUTION": Shows count of places by price level.
        target_amenity: (Required for AMENITY_GAP) The column to check.
                        Examples: "drive_through", "outdoor_seating", "delivery",
                        "wheelchair_accessible_entrance", "serves_vegetarian_food".

    Returns:
        dict: Structured analysis results by Postal Code.
    """
    try:
        # Configuration
        table_id = os.environ.get("PLACES_BQ_TABLE_ID", "your-project.places_insights_us_sample.places")
        client = bigquery.Client()
        
        # ---------------------------------------------------------
        # MODE 1: AMENITY GAP ANALYSIS
        # "Where are the coffee shops that lack drive-throughs?"
        # ---------------------------------------------------------
        if analysis_mode == "AMENITY_GAP":
            if not target_amenity:
                return {"status": "error", "error_message": "target_amenity is required for AMENITY_GAP mode."}

            # Safe column validation to prevent injection
            allowed_cols = {
                "drive_through", "outdoor_seating", "delivery", "takeout", "dine_in",
                "serves_breakfast", "serves_lunch", "serves_dinner", 
                "wheelchair_accessible_entrance", "serves_vegetarian_food"
            }
            if target_amenity not in allowed_cols:
                return {"status": "error", "error_message": f"Invalid amenity. Allowed: {allowed_cols}"}

            query = f"""
                SELECT WITH AGGREGATION_THRESHOLD
                    postal_code,
                    COUNT(*) as total_competitors,
                    COUNTIF({target_amenity} = TRUE) as has_amenity,
                    COUNTIF({target_amenity} = FALSE OR {target_amenity} IS NULL) as lacks_amenity,
                    ROUND(COUNTIF({target_amenity} = TRUE) / COUNT(*) * 100, 1) as saturation_percentage
                FROM `{table_id}`
                WHERE city = @city AND primary_type = @type
                GROUP BY postal_code
                ORDER BY lacks_amenity DESC
                LIMIT 10
            """

        # ---------------------------------------------------------
        # MODE 2: VULNERABILITY ANALYSIS (Weak Incumbents)
        # "Where are competitors busy (high reviews) but bad (low rating)?"
        # ---------------------------------------------------------
        elif analysis_mode == "VULNERABILITY":
            query = f"""
                SELECT WITH AGGREGATION_THRESHOLD
                    postal_code,
                    COUNT(*) as competitor_count,
                    ROUND(AVG(rating), 2) as avg_rating,
                    SUM(user_rating_count) as total_review_volume,
                    -- 'Vulnerability Score': High volume / Low rating
                    ROUND(SUM(user_rating_count) / NULLIF(AVG(rating), 0), 0) as vulnerability_index
                FROM `{table_id}`
                WHERE city = @city AND primary_type = @type
                GROUP BY postal_code
                HAVING avg_rating < 4.0  -- Filter for generally poor performance
                ORDER BY vulnerability_index DESC
                LIMIT 10
            """

        # ---------------------------------------------------------
        # MODE 3: PRICE DISTRIBUTION
        # "Is this a luxury area or a budget area?"
        # ---------------------------------------------------------
        elif analysis_mode == "PRICE_DISTRIBUTION":
            query = f"""
                SELECT WITH AGGREGATION_THRESHOLD
                    postal_code,
                    COUNTIF(price_level = 'PRICE_LEVEL_INEXPENSIVE') as budget_count,
                    COUNTIF(price_level = 'PRICE_LEVEL_MODERATE') as moderate_count,
                    COUNTIF(price_level = 'PRICE_LEVEL_EXPENSIVE' OR price_level = 'PRICE_LEVEL_VERY_EXPENSIVE') as luxury_count
                FROM `{table_id}`
                WHERE city = @city AND primary_type = @type
                GROUP BY postal_code
                ORDER BY moderate_count DESC
                LIMIT 10
            """
        
        else:
            return {"status": "error", "error_message": "Invalid analysis_mode."}

        # Execute Query
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("city", "STRING", city),
                bigquery.ScalarQueryParameter("type", "STRING", business_type),
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        rows = [dict(row) for row in query_job.result()]

        return {
            "status": "success",
            "analysis_mode": analysis_mode,
            "city": city,
            "results": rows
        }

    except Exception as e:
        return {"status": "error", "error_message": str(e)}
    

"""Tools for Market Research Agent."""


def get_price_segmentation(city: str, business_type: str, tool_context: ToolContext) -> dict:
    """
    Analyzes the economic price tiers of a city for a specific business type.

    Use this tool to determine if a neighborhood is 'Budget', 'Moderate', or 'Luxury'.
    It queries the Google Maps Places Insights dataset to count businesses by price level.

    Args:
        city: The city to analyze (e.g., "San Francisco").
        business_type: The type of business (e.g., "coffee_shop", "gym", "restaurant").

    Returns:
        dict: A summary of price distribution per zip code.
    """
    print(f"Analyzing price segmentation for {business_type} in {city}...")
    
    # Call the backend with the specific mode hardcoded
    raw_data = analyze_market_gaps(
        city=city, 
        business_type=business_type, 
        analysis_mode="PRICE_DISTRIBUTION",
        tool_context=tool_context
    )

    if raw_data.get("status") == "error":
        return raw_data

    # Format the output specifically for the Market Research Agent
    formatted_results = []
    for area in raw_data.get("results", []):
        zip_code = area.get('postal_code')
        luxury = area.get('luxury_count', 0)
        moderate = area.get('moderate_count', 0)
        budget = area.get('budget_count', 0)
        
        # Determine dominant segment
        if luxury > (budget + moderate):
            segment = "Luxury Dominant"
        elif budget > (luxury + moderate):
            segment = "Budget Dominant"
        else:
            segment = "Mixed/Competitive"

        formatted_results.append({
            "zip_code": zip_code,
            "market_segment": segment,
            "details": f"Lux:{luxury}, Mod:{moderate}, Bud:{budget}"
        })

    return {
        "status": "success",
        "summary": formatted_results
    }

"""Tools for Competitor Mapping Agent."""


def find_competitor_weaknesses(
    city: str, 
    business_type: str, 
    check_amenity: str = None, 
    tool_context: ToolContext = None
) -> dict:
    """
    Identifies vulnerable competitors and missing amenities in a city.

    Use this tool to find 'soft targets' (areas with low-rated competitors) 
    and 'feature gaps' (areas where competitors lack a specific amenity).

    Args:
        city: The city to analyze (e.g., "Austin").
        business_type: The business type (e.g., "coffee_shop").
        check_amenity: Optional. A specific feature to check for saturation.
                       Valid: "drive_through", "outdoor_seating", "delivery", 
                       "serves_vegetarian_food".

    Returns:
        dict: Lists of vulnerable zip codes and amenity gaps.
    """
    print(f"Scanning for competitor weaknesses in {city}...")
    
    insights = {
        "vulnerable_areas": [],
        "amenity_gaps": []
    }

    # 1. Run Vulnerability Scan (Always run this)
    vuln_data = analyze_market_gaps(
        city=city, 
        business_type=business_type, 
        analysis_mode="VULNERABILITY",
        tool_context=tool_context
    )
    
    if vuln_data.get("status") == "success":
        for area in vuln_data.get("results", []):
            insights["vulnerable_areas"].append({
                "zip_code": area.get('postal_code'),
                "reason": "High Volume / Low Rating",
                "stats": f"Avg Rating: {area.get('avg_rating')} ({area.get('total_review_volume')} reviews)"
            })

    # 2. Run Amenity Gap Scan (Only if requested)
    if check_amenity:
        gap_data = analyze_market_gaps(
            city=city, 
            business_type=business_type, 
            analysis_mode="AMENITY_GAP", 
            target_amenity=check_amenity,
            tool_context=tool_context
        )
        
        if gap_data.get("status") == "success":
            for area in gap_data.get("results", []):
                # Only report if saturation is low (< 30%)
                saturation = area.get('saturation_percentage', 100)
                if saturation < 30:
                    insights["amenity_gaps"].append({
                        "zip_code": area.get('postal_code'),
                        "opportunity": f"Lack of {check_amenity}",
                        "details": f"Only {saturation}% of competitors have this."
                    })

    return {
        "status": "success",
        "insights": insights
    }