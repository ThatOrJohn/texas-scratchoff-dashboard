import pandas as pd
import numpy as np
from datetime import datetime


class DataProcessor:
    """
    Class to process data from Neo4j and prepare it for visualization
    """

    def __init__(self, neo4j_connector):
        """
        Initialize the data processor

        Parameters:
        -----------
        neo4j_connector : Neo4jConnector
            Connector to the Neo4j database
        """
        self.connector = neo4j_connector
        # Store reference in both variable names for compatibility
        self.neo4j_connector = neo4j_connector

    def get_all_games(self):
        """
        Get all games data as a DataFrame

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing all games
        """
        games_data = self.connector.get_games()
        if not games_data:
            return pd.DataFrame()

        df = pd.DataFrame(games_data)

        # Convert date fields if they exist
        date_columns = ['start_date', 'end_date', 'last_updated']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        # Ensure numeric types for price
        if 'ticket_price' in df.columns:
            df['ticket_price'] = pd.to_numeric(
                df['ticket_price'], errors='coerce')

        return df

    def get_all_prizes(self):
        """
        Get all prizes data as a DataFrame

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing all prizes
        """
        prizes_data = self.connector.get_prize_details() if hasattr(
            self.connector, 'get_prize_details') else []
        if not prizes_data:
            return pd.DataFrame()

        df = pd.DataFrame(prizes_data)

        # Ensure numeric types
        numeric_columns = ['prize_level', 'total_count',
                           'claimed_count', 'remaining_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Add prize amount based on prize level if missing
        # This is an estimation based on prize level, as actual amounts aren't in the schema
        if 'prize_amount' not in df.columns and 'prize_level' in df.columns:
            # Convert prize level to numeric, treating "1" as highest level
            df['prize_amount'] = df['prize_level'].apply(
                lambda x: float(10000/float(x)) if x and float(x) > 0 else 0
            )

        return df

    def get_combined_data(self):
        """
        Get combined games and prizes data with additional calculated fields

        Returns:
        --------
        pandas.DataFrame
            DataFrame with combined game and prize information
        """
        combined_data = self.connector.get_games_with_prize_details() if hasattr(
            self.connector, 'get_games_with_prize_details') else []
        if not combined_data:
            return pd.DataFrame()

        df = pd.DataFrame(combined_data)

        # Convert date fields if they exist
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(
                df['last_updated'], format='%m/%d/%Y', errors='coerce')

        # Ensure numeric types
        numeric_columns = ['ticket_price', 'prize_level',
                           'total_count', 'claimed_count', 'remaining_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Add prize amount based on prize level if missing
        # This is an estimation based on prize level, as actual amounts aren't in the schema
        if 'prize_amount' not in df.columns and 'prize_level' in df.columns:
            # Convert prize level to numeric, treating "1" as highest level
            df['prize_amount'] = df['prize_level'].apply(
                lambda x: float(10000/float(x)) if x and float(x) > 0 else 0
            )

        # Calculate additional fields
        return self._calculate_additional_fields(df)

    def get_games_to_avoid(self):
        """
        Get games where 90% or more of the top prizes have been claimed

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing games to avoid
        """
        if hasattr(self.connector, 'get_games_to_avoid'):
            games_to_avoid = self.connector.get_games_to_avoid()
        else:
            games_to_avoid = []

        if not games_to_avoid:
            return pd.DataFrame()

        df = pd.DataFrame(games_to_avoid)

        # Ensure numeric types
        numeric_columns = ['prize_level', 'ticket_price',
                           'total_prizes', 'prizes_claimed', 'claim_rate']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Format game names to include game_id for disambiguation
        if all(col in df.columns for col in ['game_name', 'game_id']):
            df['formatted_game_name'] = df.apply(
                lambda row: f"{row['game_name']} ({row['game_id']})", axis=1
            )

        return df

    def get_filtered_data(self, game_id=None, min_ticket_price=1, max_ticket_price=100, ending_filter='include'):
        """
        Get filtered data based on specified criteria

        Parameters:
        -----------
        game_id : str, optional
            Filter by specific game ID
        min_ticket_price : float, optional
            Minimum ticket price for filtering Game nodes
        max_ticket_price : float, optional
            Maximum ticket price for filtering Game nodes
        ending_filter : str, optional
            Filter for games ending soon ('include', 'exclude', or 'only')

        Returns:
        --------
        pandas.DataFrame
            Filtered DataFrame with calculated fields
        """
        if hasattr(self.connector, 'get_filtered_games'):
            filtered_data = self.connector.get_filtered_games(
                game_id=game_id,
                min_ticket_price=min_ticket_price,
                max_ticket_price=max_ticket_price,
                ending_filter=ending_filter
            )
        else:
            filtered_data = []

        if not filtered_data:
            return pd.DataFrame()

        df = pd.DataFrame(filtered_data)

        # Convert date fields if they exist
        if 'last_updated' in df.columns:
            df['last_updated'] = pd.to_datetime(
                df['last_updated'], format='%m/%d/%Y', errors='coerce')

        # Ensure numeric types
        numeric_columns = ['ticket_price', 'prize_level',
                           'total_count', 'claimed_count', 'remaining_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Add prize amount based on prize level if missing
        # This is an estimation based on prize level, as actual amounts aren't in the schema
        if 'prize_amount' not in df.columns and 'prize_level' in df.columns:
            # Convert prize level to numeric, treating "1" as highest level
            df['prize_amount'] = df['prize_level'].apply(
                lambda x: float(10000/float(x)) if x and float(x) > 0 else 0
            )

        # Calculate additional fields
        return self._calculate_additional_fields(df)

    def _calculate_additional_fields(self, df):
        """
        Calculate additional fields for analysis

        Parameters:
        -----------
        df : pandas.DataFrame
            DataFrame with raw game and prize data

        Returns:
        --------
        pandas.DataFrame
            DataFrame with additional calculated fields
        """
        if df.empty:
            return df

        # Format game names to include game_id for disambiguation if both columns exist
        if all(col in df.columns for col in ['game_name', 'game_id']):
            df['formatted_game_name'] = df.apply(
                lambda row: f"{row['game_name']} ({row['game_id']})", axis=1
            )

        # First, make sure remaining_count is calculated properly from total and claimed
        if all(col in df.columns for col in ['total_count', 'claimed_count']):
            # Recalculate remaining count directly
            df['remaining_count'] = df['total_count'] - df['claimed_count']

            # Calculate unclaimed prizes (same as remaining count)
            df['unclaimed_prizes'] = df['remaining_count']

        # Calculate win probability
        if all(col in df.columns for col in ['remaining_count', 'total_count']):
            # Avoid division by zero
            df['win_probability'] = np.where(
                df['remaining_count'] > 0,
                df['remaining_count'] / df['total_count'],
                0
            )

        # Calculate expected value
        if all(col in df.columns for col in ['win_probability', 'prize_amount', 'ticket_price']):
            df['expected_value'] = df['win_probability'] * \
                df['prize_amount'] - df['ticket_price']

        # Aggregate prize data by game if necessary
        if 'game_id' in df.columns and len(df) > len(df['game_id'].unique()):
            agg_dict = {
                'game_name': 'first',
                'ticket_price': 'first',
                'total_count': 'sum',
                'claimed_count': 'sum',
                'remaining_count': 'sum',
                'unclaimed_prizes': 'sum',
                'last_updated': 'max'
            }

            # Include formatted_game_name if it exists
            if 'formatted_game_name' in df.columns:
                agg_dict['formatted_game_name'] = 'first'

            game_aggregates = df.groupby('game_id').agg(agg_dict).reset_index()

            # Calculate aggregated win probability and expected value
            if 'remaining_count' in game_aggregates.columns and 'total_count' in game_aggregates.columns:
                game_aggregates['win_probability'] = np.where(
                    game_aggregates['total_count'] > 0,
                    game_aggregates['remaining_count'] /
                    game_aggregates['total_count'],
                    0
                )

            # Expected value calculation would need prize amount information which is lost in aggregation
            # We'll need to calculate it differently or omit it from the aggregated data

            return game_aggregates

        return df
