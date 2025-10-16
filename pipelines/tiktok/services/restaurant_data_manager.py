"""
Restaurant Data Manager for TikTok pipeline.
"""

import pandas as pd
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from thefuzz import fuzz

from ..config import (
    RESTAURANT_CONFIG,
    DATA_DIR,
    LOGS_DIR
)
from ..utils.logger import setup_logger

logger = setup_logger(__name__)

class RestaurantDataManager:
    """Class for managing restaurant data loading and querying operations."""
    
    def __init__(self, data_file_path: Optional[Union[str, Path]] = None):
        """
        Initialize the restaurant data manager.
        
        Args:
            data_file_path: Path to file containing restaurant data.
                      If provided, data will be loaded immediately.
        """
        self.restaurants_df = None
        
        if data_file_path:
            self.load_data(data_file_path)
    
    def load_data(
        self, 
        file_path: Union[str, Path], 
        file_format: Optional[str] = None,
        limit: Optional[int] = None
    ) -> bool:
        """
        Load restaurant data from CSV, Parquet, or Feather file.
        
        Args:
            file_path: Path to the data file
            file_format: Format of the file ('csv', 'parquet', 'feather'). 
                        If None, will be inferred from file extension.
            limit: Maximum number of restaurants to load (for testing)
                
        Returns:
            True if data loaded successfully, False otherwise
        """
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"File not found at {file_path}")
            return False
            
        try:
            # Determine file format if not specified
            if file_format is None:
                extension = file_path.suffix.lower()
                if extension == '.csv':
                    file_format = 'csv'
                elif extension in ('.parquet', '.pq'):
                    file_format = 'parquet'
                elif extension in ('.feather', '.ftr'):
                    file_format = 'feather'
                else:
                    logger.error(f"Unsupported file format: {extension}")
                    return False
            
            if file_format not in RESTAURANT_CONFIG['supported_formats']:
                logger.error(f"Unsupported file format: {file_format}")
                return False
            
            # Load data according to format
            if file_format == 'csv':
                if limit and isinstance(limit, int) and limit > 0:
                    self.restaurants_df = pd.read_csv(file_path, nrows=limit)
                else:
                    self.restaurants_df = pd.read_csv(file_path)
            elif file_format == 'parquet':
                if limit and isinstance(limit, int) and limit > 0:
                    self.restaurants_df = pd.read_parquet(file_path).head(limit)
                else:
                    self.restaurants_df = pd.read_parquet(file_path)
            elif file_format == 'feather':
                if limit and isinstance(limit, int) and limit > 0:
                    self.restaurants_df = pd.read_feather(file_path).head(limit)
                else:
                    self.restaurants_df = pd.read_feather(file_path)
            
            # Reset index and process data
            self.restaurants_df.reset_index(inplace=True)
            self.restaurants_df.rename(columns={"index": "id"}, inplace=True)
            
            # Process types field if it exists
            if 'types' in self.restaurants_df.columns:
                self.restaurants_df['types_list'] = self.restaurants_df['types'].apply(
                    lambda x: eval(x) if isinstance(x, str) else []
                )
            
            # Generate search keywords
            self._generate_search_keywords()
            
            logger.info(f"Successfully loaded {len(self.restaurants_df)} restaurants from {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading restaurant data: {e}")
            return False
    
    def _generate_search_keywords(self) -> None:
        """
        Generate search keywords for each restaurant based on name and vicinity.
        """
        if self.restaurants_df is None:
            logger.error("No restaurant data loaded")
            return
            
        try:
            # Create search keywords from name and vicinity
            if 'name' in self.restaurants_df.columns and 'vicinity' in self.restaurants_df.columns:
                self.restaurants_df['search_keywords'] = self.restaurants_df.apply(
                    lambda row: f"{row['name']} {row['vicinity']} food",
                    axis=1
                )
            elif 'name' in self.restaurants_df.columns:
                self.restaurants_df['search_keywords'] = self.restaurants_df.apply(
                    lambda row: f"{row['name']} food",
                    axis=1
                )
            else:
                logger.warning("Cannot generate search keywords, missing required columns")
                
        except Exception as e:
            logger.error(f"Error generating search keywords: {e}")
    
    def get_restaurant_count(self) -> int:
        """
        Get the number of restaurants loaded.
        
        Returns:
            Number of restaurants in the dataframe
        """
        if self.restaurants_df is None:
            return 0
        return len(self.restaurants_df)
    
    def get_restaurant(self, index: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific restaurant by index.
        
        Args:
            index: Index of the restaurant in the dataframe
            
        Returns:
            Dictionary with restaurant data or None if not found
        """
        if self.restaurants_df is None or index < 0 or index >= len(self.restaurants_df):
            return None
            
        try:
            return self.restaurants_df.iloc[index].to_dict()
        except Exception as e:
            logger.error(f"Error retrieving restaurant at index {index}: {e}")
            return None
    
    def get_restaurant_by_id(self, restaurant_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """
        Get a specific restaurant by ID.
        
        Args:
            restaurant_id: ID of the restaurant
            
        Returns:
            Dictionary with restaurant data or None if not found
        """
        if self.restaurants_df is None:
            return None
            
        try:
            result = self.restaurants_df[self.restaurants_df['id'] == restaurant_id]
            if len(result) == 0:
                return None
            return result.iloc[0].to_dict()
        except Exception as e:
            logger.error(f"Error retrieving restaurant with ID {restaurant_id}: {e}")
            return None
    
    def get_restaurants_by_type(self, restaurant_type: str) -> List[Dict[str, Any]]:
        """
        Get restaurants that match a specific type.
        
        Args:
            restaurant_type: Type of restaurant to search for
            
        Returns:
            List of matching restaurants
        """
        if self.restaurants_df is None or 'types_list' not in self.restaurants_df.columns:
            return []
            
        try:
            # Filter restaurants by type
            matching_restaurants = self.restaurants_df[
                self.restaurants_df['types_list'].apply(
                    lambda types: restaurant_type in types if isinstance(types, list) else False
                )
            ]
            
            # Convert to list of dictionaries
            return matching_restaurants.to_dict('records')
        except Exception as e:
            logger.error(f"Error retrieving restaurants of type {restaurant_type}: {e}")
            return []
    
    def search_restaurants(
        self, 
        query: str, 
        threshold: int = RESTAURANT_CONFIG['search_threshold']
    ) -> List[Dict[str, Any]]:
        """
        Search for restaurants by name using fuzzy matching.
        
        Args:
            query: Search query
            threshold: Minimum match score (0-100) to include in results
            
        Returns:
            List of matching restaurants with scores
        """
        if self.restaurants_df is None:
            return []
            
        try:
            results = []
            
            # Perform fuzzy matching on restaurant names
            for _, row in self.restaurants_df.iterrows():
                restaurant_name = row.get('name', '')
                if not restaurant_name:
                    continue
                    
                score = fuzz.ratio(query.lower(), restaurant_name.lower())
                
                if score >= threshold:
                    restaurant_data = row.to_dict()
                    restaurant_data['match_score'] = score
                    results.append(restaurant_data)
            
            # Sort by match score (highest first)
            results.sort(key=lambda x: x['match_score'], reverse=True)
            return results
            
        except Exception as e:
            logger.error(f"Error searching restaurants: {e}")
            return []
    
    def get_all_restaurant_data(self) -> List[Dict[str, Any]]:
        """
        Get all restaurant data as a list of dictionaries.
        
        Returns:
            List of all restaurant data
        """
        if self.restaurants_df is None:
            return []
            
        try:
            return self.restaurants_df.to_dict('records')
        except Exception as e:
            logger.error(f"Error retrieving all restaurant data: {e}")
            return []