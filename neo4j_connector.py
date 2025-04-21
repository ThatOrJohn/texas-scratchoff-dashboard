import os
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

class Neo4jConnector:
    """
    Class to handle connections and queries to a Neo4j database via the Bolt protocol
    """
    
    def __init__(self, uri, username, password):
        """
        Initialize the connector with connection parameters
        
        Parameters:
        -----------
        uri : str
            The URI for the Neo4j Bolt endpoint (e.g., bolt://localhost:7687 or neo4j+s://xxx.databases.neo4j.io)
        username : str
            Neo4j username
        password : str
            Neo4j password
        """
        self.uri = uri
        self.username = username
        self.password = password
        self.driver = None
        
        # Try to establish the driver connection
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
        except Exception as e:
            print(f"Driver initialization error: {str(e)}")
    
    def test_connection(self):
        """
        Test the connection to the Neo4j database
        
        Returns:
        --------
        bool
            True if connection is successful, False otherwise
        """
        if not self.driver:
            return False
            
        try:
            # Verify connectivity by running a simple query
            with self.driver.session() as session:
                result = session.run("RETURN 1 AS test")
                record = result.single()
                return record and record["test"] == 1
        except (ServiceUnavailable, AuthError) as e:
            print(f"Connection test failed: {str(e)}")
            return False
        except Exception as e:
            print(f"Unexpected error during connection test: {str(e)}")
            return False
    
    def execute_query(self, query, params=None):
        """
        Execute a Cypher query against the Neo4j database
        
        Parameters:
        -----------
        query : str
            The Cypher query to execute
        params : dict, optional
            Parameters for the query
            
        Returns:
        --------
        dict
            Dictionary with 'columns' and 'data' keys mimicking the REST API response format
        """
        if params is None:
            params = {}
        
        # Default empty result    
        empty_result = {
            'columns': [],
            'data': []
        }
            
        if not self.driver:
            print("Driver not initialized. Check connection parameters.")
            return empty_result
            
        try:
            with self.driver.session() as session:
                result = session.run(query, params)
                
                # Collect columns
                columns = result.keys()
                
                # Collect data rows
                data = []
                for record in result:
                    row = []
                    for column in columns:
                        row.append(record[column])
                    data.append(row)
                
                # Format the result as a dictionary similar to the REST API format
                # This maintains compatibility with the existing code
                return {
                    'columns': columns,
                    'data': data
                }
        except Exception as e:
            print(f"Query execution error: {str(e)}")
            # Return empty result instead of raising exception to avoid app crashes
            return empty_result
            
    def close(self):
        """
        Close the Neo4j driver connection
        """
        if self.driver:
            self.driver.close()
    
    def get_games(self):
        """
        Get all lottery games from the database
        
        Returns:
        --------
        list
            List of game dictionaries
        """
        query = """
        MATCH (g:Game)
        RETURN g.game_number AS game_id, g.game_name AS game_name, g.ticket_price AS ticket_price, 
               g.date_updated AS last_updated, g.game_close_date AS game_close_date,
               g.total_prizes AS total_prizes, g.total_prizes AS total_count, 
               g.prizes_claimed AS prizes_claimed, g.prizes_claimed AS claimed_count
        """
        
        result = self.execute_query(query)
        
        if 'data' not in result:
            return []
            
        games = []
        columns = result['columns']
        
        for row in result['data']:
            game_dict = {columns[i]: value for i, value in enumerate(row)}
            games.append(game_dict)
            
        return games
    
    def get_prize_details(self):
        """
        Get all prize detail information from the database
        
        Returns:
        --------
        list
            List of prize detail dictionaries
        """
        query = """
        MATCH (d:Detail)
        RETURN d.game_number AS game_id, d.prize_level AS prize_level, 
               d.total_prizes AS total_prizes, d.total_prizes AS total_count, 
               d.prizes_claimed AS prizes_claimed, d.prizes_claimed AS claimed_count
        """
        
        result = self.execute_query(query)
        
        if 'data' not in result:
            return []
            
        prizes = []
        columns = result['columns']
        
        for row in result['data']:
            prize_dict = {columns[i]: value for i, value in enumerate(row)}
            # Calculate remaining count with None handling
            if 'total_count' in prize_dict and 'claimed_count' in prize_dict:
                total_count = 0 if prize_dict['total_count'] is None else prize_dict['total_count']
                claimed_count = 0 if prize_dict['claimed_count'] is None else prize_dict['claimed_count']
                prize_dict['remaining_count'] = total_count - claimed_count
            prizes.append(prize_dict)
            
        return prizes
    
    def get_game_prize_details(self, game_id):
        """
        Get prize details for a specific game
        
        Parameters:
        -----------
        game_id : str
            The game number to query prizes for
            
        Returns:
        --------
        list
            List of prize detail dictionaries for the specified game
        """
        # Add debugging information
        print(f"Looking up game_prize_details for game_id: {game_id}")
        
        query = """
        MATCH (g:Game), (d:Detail)
        WHERE g.game_number = d.game_number AND g.game_number = $game_id
        RETURN g.game_name AS game_name, g.ticket_price AS ticket_price,
               d.prize_level AS prize_level, d.prize_level AS prize_amount,
               g.total_prizes AS total_prizes, g.total_prizes AS total_count,
               g.prizes_claimed AS prizes_claimed, g.prizes_claimed AS claimed_count, 
               d.total_prizes AS detail_total_count, d.total_prizes AS detail_total_prizes,
               d.prizes_claimed AS detail_claimed_count, d.prizes_claimed AS detail_prizes_claimed
        ORDER BY toInteger(d.prize_level) DESC
        """
        
        params = {'game_id': game_id}
        result = self.execute_query(query, params)
        
        # Debug the query result
        print(f"Query result for game {game_id}: data length = {len(result['data']) if 'data' in result else 'No data'}")
        
        if 'data' not in result:
            return []
            
        prizes = []
        columns = result['columns']
        
        for row in result['data']:
            prize_dict = {columns[i]: value for i, value in enumerate(row)}
            # Calculate remaining count with None handling
            if 'total_count' in prize_dict and 'claimed_count' in prize_dict:
                total_count = 0 if prize_dict['total_count'] is None else prize_dict['total_count']
                claimed_count = 0 if prize_dict['claimed_count'] is None else prize_dict['claimed_count']
                prize_dict['remaining_count'] = total_count - claimed_count
            prizes.append(prize_dict)
        
        # Debug the result we're returning
        print(f"Returning {len(prizes)} prize entries for game {game_id}")
        
        return prizes
    
    def get_games_with_prize_details(self):
        """
        Get games with their associated prize details by joining Game and Detail nodes
        
        Returns:
        --------
        list
            List of dictionaries containing game and prize detail information
        """
        query = """
        MATCH (g:Game), (d:Detail)
        WHERE g.game_number = d.game_number
        RETURN g.game_number AS game_id, g.game_name AS game_name, g.ticket_price AS ticket_price,
               d.prize_level AS prize_level, 
               g.total_prizes AS total_prizes, g.total_prizes AS total_count, 
               g.prizes_claimed AS prizes_claimed, g.prizes_claimed AS claimed_count, 
               g.date_updated AS last_updated, g.game_close_date AS game_close_date
        """
        
        result = self.execute_query(query)
        
        if 'data' not in result:
            return []
            
        combined_data = []
        columns = result['columns']
        
        for row in result['data']:
            data_dict = {columns[i]: value for i, value in enumerate(row)}
            # Calculate remaining count with None handling
            if 'total_count' in data_dict and 'claimed_count' in data_dict:
                total_count = 0 if data_dict['total_count'] is None else data_dict['total_count']
                claimed_count = 0 if data_dict['claimed_count'] is None else data_dict['claimed_count']
                data_dict['remaining_count'] = total_count - claimed_count
            combined_data.append(data_dict)
            
        return combined_data
    
    def get_games_to_avoid(self):
        """
        Get games where 90% or more of the top prizes have been claimed
        
        Returns:
        --------
        list
            List of games to avoid with their information
        """
        query = """
        MATCH (g:Game)<-[:BELONGS_TO]-(d:Detail)
        WITH g, max(toInteger(d.prize_level)) AS max_prize_level
        MATCH (g)<-[:BELONGS_TO]-(d:Detail)
        WHERE toInteger(d.prize_level) = max_prize_level 
        AND (toFloat(d.prizes_claimed) / d.total_prizes) >= 0.9
        RETURN g.game_name AS game_name, g.game_number AS game_id, 
               d.prize_level AS prize_level, g.ticket_price AS ticket_price,
               d.total_prizes AS total_prizes, d.prizes_claimed AS prizes_claimed, 
               (toFloat(d.prizes_claimed) / d.total_prizes) AS claim_rate
        """
        
        result = self.execute_query(query)
        
        if 'data' not in result:
            return []
            
        games_to_avoid = []
        columns = result['columns']
        
        for row in result['data']:
            game_dict = {columns[i]: value for i, value in enumerate(row)}
            games_to_avoid.append(game_dict)
            
        return games_to_avoid
        
    def get_filtered_games(self, game_id=None, min_ticket_price=1, max_ticket_price=100, ending_filter='include'):
        """
        Get filtered games and prizes based on criteria
        
        Parameters:
        -----------
        game_id : str, optional
            Filter by specific game ID (game_number)
        min_ticket_price : float, optional
            Minimum ticket price for filtering Game nodes
        max_ticket_price : float, optional
            Maximum ticket price for filtering Game nodes
        ending_filter : str, optional
            Filter for games ending soon ('include', 'exclude', or 'only')
            
        Returns:
        --------
        list
            Filtered list of game and prize data
        """
        query_parts = [
            "MATCH (g:Game), (d:Detail)",
            "WHERE g.game_number = d.game_number"
        ]
        
        params = {}
        
        if game_id:
            query_parts.append("AND g.game_number = $game_id")
            params['game_id'] = game_id
        
        # Add ticket price filter
        if min_ticket_price is not None or max_ticket_price is not None:
            if min_ticket_price is not None:
                query_parts.append("AND toFloat(g.ticket_price) >= $min_ticket_price")
                params['min_ticket_price'] = float(min_ticket_price)
            
            if max_ticket_price is not None:
                query_parts.append("AND toFloat(g.ticket_price) <= $max_ticket_price")
                params['max_ticket_price'] = float(max_ticket_price)
        
        # Add game_close_date filter based on ending_filter parameter
        if ending_filter != 'include':
            if ending_filter == 'only':
                # Only show games ending soon - with a valid non-null, non-empty game_close_date
                query_parts.append("AND g.game_close_date IS NOT NULL AND g.game_close_date <> '' AND g.game_close_date <> 'None' AND g.game_close_date <> 'null'")
            elif ending_filter == 'exclude':
                # Exclude games ending soon - only show games without valid game_close_date
                query_parts.append("AND (g.game_close_date IS NULL OR g.game_close_date = '' OR g.game_close_date = 'None' OR g.game_close_date = 'null')")
            
        query_parts.append("""
        RETURN g.game_number AS game_id, g.game_name AS game_name, g.ticket_price AS ticket_price,
               d.prize_level AS prize_level, 
               g.total_prizes AS total_prizes, g.total_prizes AS total_count, 
               g.prizes_claimed AS prizes_claimed, g.prizes_claimed AS claimed_count, 
               g.date_updated AS last_updated, g.game_close_date AS game_close_date
        """)
        
        query = " ".join(query_parts)
        result = self.execute_query(query, params)
        
        if 'data' not in result:
            return []
            
        filtered_data = []
        columns = result['columns']
        
        for row in result['data']:
            data_dict = {columns[i]: value for i, value in enumerate(row)}
            # Calculate remaining count with None handling
            if 'total_count' in data_dict and 'claimed_count' in data_dict:
                total_count = 0 if data_dict['total_count'] is None else data_dict['total_count']
                claimed_count = 0 if data_dict['claimed_count'] is None else data_dict['claimed_count']
                data_dict['remaining_count'] = total_count - claimed_count
            filtered_data.append(data_dict)
            
        return filtered_data