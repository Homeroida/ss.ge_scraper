"""
Data processing service for real estate data.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional

from utils.logging_utils import setup_logger

logger = setup_logger(__name__)

class RealEstateDataProcessor:
    """
    Processor for real estate data.
    """
    
    @staticmethod
    def extract_property_info(prop: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant information from a property listing.
        
        Args:
            prop: Property data dictionary.
            
        Returns:
            Dictionary with extracted and processed property information.
        """
        # Extract main image if available
        main_image = next(
            (img.get("fileName") for img in prop.get("appImages", []) 
             if img.get("isMain")), 
            None
        )
        
        return {
            "ID": prop.get("applicationId"),
            "Title": prop.get("title"),
            "Price_GEL": prop.get("price", {}).get("priceGeo"),
            "Price_USD": prop.get("price", {}).get("priceUsd"),
            "PricePerSqm_GEL": prop.get("price", {}).get("unitPriceGeo"),
            "PricePerSqm_USD": prop.get("price", {}).get("unitPriceUsd"),
            "Area_SqM": prop.get("totalArea"),
            "City": prop.get("address", {}).get("cityTitle"),
            "District": prop.get("address", {}).get("districtTitle"),
            "Subdistrict": prop.get("address", {}).get("subdistrictTitle"),
            "Street": prop.get("address", {}).get("streetTitle"),
            "Description": (prop.get("description") or "").strip()[:300],
            "MainImage": main_image,
            "URL": f"https://home.ss.ge/real-estate/{prop.get('applicationId')}",
        }
    
    def process_data(
        self, 
        properties: List[Dict[str, Any]], 
        output_csv: Path
    ) -> Optional[pd.DataFrame]:
        """
        Process raw property data into a cleaned DataFrame.
        
        Args:
            properties: List of property data dictionaries.
            output_csv: Path to save the processed CSV file.
            
        Returns:
            Pandas DataFrame with processed data or None if processing fails.
        """
        if not properties:
            logger.warning("No properties to process")
            return None
        
        logger.info(f"Processing {len(properties)} properties...")
        
        try:
            # Extract relevant information from each property
            processed = [self.extract_property_info(p) for p in properties]
            
            # Create DataFrame and clean data
            df = pd.DataFrame(processed)
            df = df.dropna(subset=["Price_GEL"])
            
            # Convert numeric columns
            numeric_cols = ["Price_GEL", "Price_USD", "PricePerSqm_GEL", 
                           "PricePerSqm_USD", "Area_SqM"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            # Calculate price per square meter if necessary
            if "Price_USD" in df.columns and "Area_SqM" in df.columns:
                df["Price_Per_SqM_USD"] = (df["Price_USD"] / df["Area_SqM"]).round(2)
            
            # Save to CSV
            df.to_csv(output_csv, index=False)
            logger.info(f"Saved processed data to {output_csv}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return None