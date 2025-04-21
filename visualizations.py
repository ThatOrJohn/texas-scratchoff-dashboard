import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from utils import format_currency


class Visualizations:
    """
    Class to create visualizations for the Texas Lottery dashboard
    """

    def __init__(self, data):
        """
        Initialize visualizations with data

        Parameters:
        -----------
        data : pandas.DataFrame
            DataFrame containing lottery data to visualize
        """
        self.data = data

    def create_prize_availability_chart(self, limit=20, games_ending_filter='include'):
        """
        Create a chart showing prize availability by game

        Parameters:
        -----------
        limit : int, optional
            Maximum number of games to display (default 20)
        games_ending_filter : str, optional
            Filter for games ending soon ('include', 'exclude', or 'only')

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure for prize availability
        """
        if self.data.empty or not all(col in self.data.columns for col in ['game_name']):
            # Return empty figure if data is missing
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for prize availability chart",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Prepare data for stacked bar chart
        chart_data = self.data.copy()

        # Apply ending soon filter based on game_close_date column
        # This is the only column that contains end date information in the database
        if games_ending_filter != 'include' and 'game_close_date' in chart_data.columns:
            if games_ending_filter == 'only':
                # Only show games ending soon
                chart_data = chart_data[chart_data['game_close_date'].notna()]
            elif games_ending_filter == 'exclude':
                # Exclude games ending soon
                chart_data = chart_data[chart_data['game_close_date'].isna()]

        # Group by game if there are multiple prize levels per game, making sure to use Game-level prize data
        if len(chart_data) > len(chart_data['game_name'].unique()):
            # Make sure required columns are present
            if all(col in chart_data.columns for col in ['claimed_count', 'total_count']):
                # For each game, take the first row's claimed_count and total_count as they should come from the Game node
                # (not the sum across all Detail nodes)
                agg_dict = {
                    # Take first occurrence (from Game node)
                    'claimed_count': 'first',
                    # Take first occurrence (from Game node)
                    'total_count': 'first',
                    'ticket_price': 'first'    # Keep ticket price info
                }

                # Include game_close_date column if it exists in the data
                if 'game_close_date' in chart_data.columns:
                    agg_dict['game_close_date'] = 'first'

                # Aggregate at the game level
                chart_data = chart_data.groupby(
                    'game_name').agg(agg_dict).reset_index()

                # Calculate remaining count - for each game this is total prizes minus claimed prizes
                chart_data['remaining_count'] = chart_data['total_count'] - \
                    chart_data['claimed_count']
            else:
                # If data is missing, return empty figure
                fig = go.Figure()
                fig.add_annotation(
                    text="Data missing required columns for prize availability calculation",
                    showarrow=False,
                    font=dict(size=16)
                )
                return fig

        # Sort by remaining count for better visualization
        chart_data = chart_data.sort_values('remaining_count', ascending=False)

        # Limit the number of games to display
        if len(chart_data) > limit:
            chart_data = chart_data.head(limit)

        # Create stacked bar chart
        fig = go.Figure()

        # First add claimed prizes (red)
        fig.add_trace(go.Bar(
            y=chart_data['game_name'],
            x=chart_data['claimed_count'],
            name='Claimed Prizes',
            orientation='h',
            marker_color='red',
            hovertemplate='Game: %{y}<br>Claimed Prizes: %{x:,}<extra></extra>'
        ))

        # Then add remaining prizes (green)
        fig.add_trace(go.Bar(
            y=chart_data['game_name'],
            x=chart_data['remaining_count'],
            name='Remaining Prizes',
            orientation='h',
            marker_color='green',
            hovertemplate='Game: %{y}<br>Remaining Prizes: %{x:,}<extra></extra>'
        ))

        # Update layout
        title_text = 'Prize Availability by Game'
        if limit < len(self.data['game_name'].unique()):
            title_text += f' (Top {limit})'

        if games_ending_filter == 'only':
            title_text += ' (Ending Soon)'
        elif games_ending_filter == 'exclude':
            title_text += ' (Excluding Ending Soon)'

        fig.update_layout(
            barmode='stack',
            title=title_text,
            xaxis_title='Number of Prizes',
            yaxis_title='Game',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            # Adjust height based on number of games
            height=max(500, 25 * len(chart_data))
        )

        # Format x-axis tick labels with commas for thousands
        fig.update_xaxes(tickformat=',d')

        return fig

    def create_expected_value_chart(self):
        """
        Create a chart showing expected value by game

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure for expected value
        """
        if self.data.empty or 'expected_value' not in self.data.columns or 'game_name' not in self.data.columns:
            # Return empty figure if data is missing
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for expected value chart",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Prepare data for bar chart
        chart_data = self.data.copy()

        # Group by game and use Game-level attributes if there are multiple prize levels per game
        if len(chart_data) > len(chart_data['game_name'].unique()):
            agg_dict = {
                'expected_value': 'first',  # Use Game-level expected value rather than mean
                'ticket_price': 'first'     # To include in hover data
            }

            # Include prize counts if available
            if 'claimed_count' in chart_data.columns:
                # Take Game node's claimed count
                agg_dict['claimed_count'] = 'first'
            if 'total_count' in chart_data.columns:
                # Take Game node's total count
                agg_dict['total_count'] = 'first'

            chart_data = chart_data.groupby(
                'game_name').agg(agg_dict).reset_index()

        # Sort by expected value
        chart_data = chart_data.sort_values('expected_value', ascending=False)

        # Limit to top 10 games if there are many
        if len(chart_data) > 10:
            chart_data = chart_data.head(10)

        # Format expected value for display
        chart_data['formatted_ev'] = chart_data['expected_value'].apply(
            format_currency)

        # Create bar chart
        fig = px.bar(
            chart_data,
            x='expected_value',
            y='game_name',
            orientation='h',
            color='expected_value',
            color_continuous_scale=px.colors.sequential.Viridis,
            labels={'expected_value': 'Expected Value', 'game_name': 'Game'},
            title='Games by Expected Value',
            text='formatted_ev'
        )

        # Add ticket price to hover info if available
        if 'ticket_price' in chart_data.columns:
            hover_template = "%{y}<br>Expected Value: %{x:$.2f}<br>Ticket Price: $%{customdata:.2f}"
            fig.update_traces(
                customdata=chart_data['ticket_price'], hovertemplate=hover_template)

        # Update layout
        fig.update_layout(
            xaxis_title='Expected Value ($)',
            yaxis_title='Game',
            coloraxis_showscale=False,
        )

        # Add a vertical line at x=0 to show break-even point
        fig.add_shape(type="line",
                      x0=0, y0=-0.5, x1=0, y1=len(chart_data)-0.5,
                      line=dict(color="red", width=2, dash="dash")
                      )

        fig.add_annotation(
            x=0, y=len(chart_data),
            text="Break-even",
            showarrow=False,
            yshift=10,
            font=dict(color="red")
        )

        return fig

    def create_prize_distribution_chart(self):
        """
        Create a chart showing prize distribution by game

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure for prize distribution
        """
        if self.data.empty or not all(col in self.data.columns for col in ['game_name', 'prize_amount']):
            # Return empty figure if data is missing
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for prize distribution chart",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Prepare data
        chart_data = self.data.copy()

        # Select top games by number of prize levels or total prize value
        top_games = chart_data.groupby(
            'game_name')['prize_amount'].sum().nlargest(5).index.tolist()
        chart_data = chart_data[chart_data['game_name'].isin(top_games)]

        # Create box plot to show prize distribution
        fig = px.box(
            chart_data,
            x='game_name',
            y='prize_amount',
            color='game_name',
            points='all',
            labels={'prize_amount': 'Prize Amount ($)', 'game_name': 'Game'},
            title='Prize Distribution by Game'
        )

        # Format y-axis as currency
        fig.update_layout(
            yaxis=dict(
                tickprefix='$',
                type='log',  # Use log scale for better visibility of different prize levels
            ),
            showlegend=False  # Hide legend as colors already distinguish games
        )

        return fig

    def create_probability_chart(self):
        """
        Create a chart showing winning probability analysis

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure for winning probability
        """
        if self.data.empty or not all(col in self.data.columns for col in ['game_name', 'win_probability']):
            # Return empty figure if data is missing
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for probability chart",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Prepare data
        chart_data = self.data.copy()

        # Group by game if there are multiple prize levels per game
        if len(chart_data) > len(chart_data['game_name'].unique()):
            agg_dict = {
                'win_probability': 'first',  # Use Game-level probability rather than mean
                'ticket_price': 'first'      # For additional context
            }

            # Include prize counts if available
            if 'claimed_count' in chart_data.columns:
                # Take Game node's claimed count
                agg_dict['claimed_count'] = 'first'
            if 'total_count' in chart_data.columns:
                # Take Game node's total count
                agg_dict['total_count'] = 'first'

            chart_data = chart_data.groupby(
                'game_name').agg(agg_dict).reset_index()

        # Sort by probability
        chart_data = chart_data.sort_values('win_probability', ascending=False)

        # Limit to top games if there are many
        if len(chart_data) > 10:
            chart_data = chart_data.head(10)

        # Format probability for display (as percentage)
        chart_data['formatted_prob'] = chart_data['win_probability'].apply(
            lambda x: f"{x:.2%}")

        # Create horizontal bar chart
        fig = px.bar(
            chart_data,
            y='game_name',
            x='win_probability',
            orientation='h',
            color='win_probability',
            color_continuous_scale=px.colors.sequential.Blues,
            labels={'win_probability': 'Win Probability', 'game_name': 'Game'},
            title='Games by Win Probability',
            text='formatted_prob'
        )

        # Add ticket price to hover info if available
        if 'ticket_price' in chart_data.columns:
            hover_template = "%{y}<br>Win Probability: %{x:.2%}<br>Ticket Price: $%{customdata:.2f}"
            fig.update_traces(
                customdata=chart_data['ticket_price'], hovertemplate=hover_template)

        # Update layout
        fig.update_layout(
            xaxis_title='Win Probability',
            yaxis_title='Game',
            coloraxis_showscale=False,
            xaxis=dict(tickformat='.0%')  # Format x-axis ticks as percentages
        )

        return fig

    def create_simulated_timeline(self):
        """
        Create a timeline chart showing games ending soon based on game_close_date

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure showing a timeline of games ending
        """
        if self.data.empty:
            # Return empty figure if data is missing
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for timeline",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Get a clean copy of the data for our timeline
        chart_data = self.data.copy()

        # Filter to only include games with a non-null or non-empty game_close_date
        if "game_close_date" in chart_data.columns:
            # Make sure to handle different types of empty values
            chart_data = chart_data[
                chart_data["game_close_date"].notna() &
                (chart_data["game_close_date"] != "") &
                (chart_data["game_close_date"] != "None") &
                (chart_data["game_close_date"] != "null")
            ]

        # Explicitly exclude problematic games from showing in the timeline chart
        if "game_name" in chart_data.columns:
            chart_data = chart_data[
                (chart_data["game_name"] != "Texas Loteria") &
                (chart_data["game_name"] != "7")
            ]

        # Add debug information to help troubleshoot
        print("\n=== TIMELINE CHART DEBUGGING ===")
        print(
            f"Total games before filtering: {len(chart_data) if not chart_data.empty else 0}")

        if "game_close_date" in chart_data.columns:
            # Check what game_close_date values we have
            print("\nAvailable game_close_date values:")
            for idx, (name, close_date) in enumerate(zip(chart_data["game_name"], chart_data["game_close_date"])):
                print(f"{idx+1}. {name}: '{close_date}' (Type: {type(close_date)})")

            # Make sure to filter out games with no valid close date
            # First, convert all empty strings, "None", "null" to actual None values
            chart_data.loc[chart_data["game_close_date"]
                           == "", "game_close_date"] = None
            chart_data.loc[chart_data["game_close_date"]
                           == "None", "game_close_date"] = None
            chart_data.loc[chart_data["game_close_date"]
                           == "null", "game_close_date"] = None

            # Then filter on not null
            chart_data = chart_data[chart_data["game_close_date"].notna()]

            # Print after filtering
            print(
                f"\nGames after filtering non-null game_close_date: {len(chart_data)}")
            if not chart_data.empty and "game_name" in chart_data.columns:
                print("Remaining games:")
                for idx, (name, close_date) in enumerate(zip(chart_data["game_name"], chart_data["game_close_date"])):
                    print(f"{idx+1}. {name}: '{close_date}'")

        # If we still have too many games, let's limit to only games with non-empty strings
        if "game_close_date" in chart_data.columns and len(chart_data) > 12:
            chart_data = chart_data[chart_data["game_close_date"].astype(
                str).str.strip() != ""]
            print(f"\nGames after filtering empty strings: {len(chart_data)}")
            if not chart_data.empty and "game_name" in chart_data.columns:
                print("Remaining games:")
                for idx, (name, close_date) in enumerate(zip(chart_data["game_name"], chart_data["game_close_date"])):
                    print(f"{idx+1}. {name}: '{close_date}'")

        # If no games with close date, return empty figure
        if chart_data.empty:
            fig = go.Figure()
            fig.add_annotation(
                text="No games ending soon to display",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # We need game_name and ticket_price
        required_columns = ['game_name', 'ticket_price']
        if not all(col in chart_data.columns for col in required_columns):
            # Return empty figure if required columns are missing
            fig = go.Figure()
            fig.add_annotation(
                text="Missing required data for timeline",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Get today's date for the reference line
        today = pd.Timestamp.now().normalize()

        # Create a simulated end date based on ticket price and claimed percentage
        # Higher ticket prices might have longer run times
        # Higher claimed percentages suggest the game might end sooner

        # Make sure we have numeric ticket prices
        chart_data['ticket_price'] = pd.to_numeric(
            chart_data['ticket_price'], errors='coerce')

        # Calculate a popularity factor based on the percentage of prizes claimed
        if all(col in chart_data.columns for col in ['claimed_count', 'total_count']):
            # Calculate percentage claimed
            chart_data['percent_claimed'] = chart_data['claimed_count'] / \
                chart_data['total_count'] * 100
            # Fill any NaN values with 50%
            chart_data['percent_claimed'] = chart_data['percent_claimed'].fillna(
                50)
        else:
            # No claim data, assign a random percentage between 30-70%
            chart_data['percent_claimed'] = np.random.uniform(
                30, 70, size=len(chart_data))

        # Simulate an end date:
        # - Base duration of 180 days (about 6 months)
        # - Higher ticket prices get longer runs (add up to 180 more days)
        # - Higher claimed percentages reduce the duration

        # Normalize ticket prices to 0-1 scale for consistent calculations
        max_price = chart_data['ticket_price'].max()
        if max_price > 0:
            price_factor = chart_data['ticket_price'] / max_price
        else:
            price_factor = 0.5  # Default if no valid ticket prices

        # Calculate days until game end
        # Base duration + price adjustment - claimed percentage adjustment
        chart_data['days_until_end'] = (
            180 +  # Base duration of 6 months
            (180 * price_factor) -  # Higher prices get longer duration
            # Higher claimed percent shortens duration
            (chart_data['percent_claimed'] * 1.5)
        ).astype(int)

        # Ensure minimum of 30 days and maximum of 365 days
        chart_data['days_until_end'] = chart_data['days_until_end'].clip(
            30, 365)

        # Calculate the end date
        chart_data['simulated_end_date'] = today + \
            pd.to_timedelta(chart_data['days_until_end'], unit='D')

        # Ensure each game has a unique position on the y-axis
        # Sort by simulated end date, with earliest ending games at the top
        chart_data = chart_data.sort_values('days_until_end')

        # Only keep top 20 games to avoid overcrowding
        if len(chart_data) > 20:
            chart_data = chart_data.head(20)

        # We need to use bar chart instead of timeline because of column name requirements
        # Create a bar chart to show game timeline
        chart_data['duration'] = (
            # in days
            chart_data['simulated_end_date'] - today).dt.total_seconds() / (24 * 60 * 60)

        fig = px.bar(
            chart_data,
            y='game_name',
            x='duration',
            orientation='h',  # horizontal bars
            # Use a single color instead of color scale
            color_discrete_sequence=['#1f77b4'],
            labels={
                'game_name': 'Game',  # Restore the game name label
                'duration': 'Days Until End'
            },
            title='Games Ending Soon'
        )

        # Adjust the x-axis to show dates instead of days
        date_ticks = []
        date_labels = []

        # Create 5 evenly spaced date ticks for x-axis
        for i in range(6):
            days = i * 60  # Every 60 days (2 months)
            date_ticks.append(days)
            date_labels.append(
                (today + pd.Timedelta(days=days)).strftime('%b %d, %Y'))

        fig.update_xaxes(
            tickvals=date_ticks,
            ticktext=date_labels
        )

        # Add today marker at x=0 since our chart starts at today (0 days)
        fig.add_vline(
            x=0,
            line_width=2,
            line_dash="dash",
            line_color="red"
        )

        # Update layout for cleaner display
        fig.update_layout(
            xaxis_title="End Date",
            yaxis_title="Game",  # Restore the Game label
            # Adjust height based on number of games
            height=max(500, 25 * len(chart_data)),
            margin=dict(l=150)  # Add left margin for game names
        )

        # Remove hover text entirely
        fig.update_layout(hovermode=False)

        return fig

    def create_games_ending_timeline(self):
        """
        This method is kept for backward compatibility, but now
        returns the simulated timeline instead since we don't have actual end dates

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure showing a timeline of games
        """
        return self.create_simulated_timeline()

    def create_prize_level_chart(self, game_name):
        """
        Create a chart showing prize levels for a specific game

        Parameters:
        -----------
        game_name : str
            Name of the game to visualize prize levels for

        Returns:
        --------
        plotly.graph_objects.Figure
            Plotly figure for prize levels
        """
        if self.data.empty or 'game_name' not in self.data.columns:
            # Return empty figure if data is missing
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for prize level chart",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Filter data for the specified game
        game_data = self.data[self.data['game_name'] == game_name].copy()

        if game_data.empty or 'prize_amount' not in game_data.columns:
            # Return empty figure if no data for the game
            fig = go.Figure()
            fig.add_annotation(
                text=f"No prize data available for {game_name}",
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        # Sort by prize amount
        game_data = game_data.sort_values('prize_amount', ascending=False)

        # Format prize amount for display
        game_data['formatted_prize'] = game_data['prize_amount'].apply(
            format_currency)

        # Create horizontal bar chart for prize levels
        fig = px.bar(
            game_data,
            y='prize_amount',
            x='remaining_count',
            orientation='v',
            color='prize_amount',
            color_continuous_scale=px.colors.sequential.Plasma,
            labels={'prize_amount': 'Prize Amount',
                    'remaining_count': 'Remaining Prizes'},
            title=f'Prize Levels for {game_name}',
            text='formatted_prize'
        )

        # Update layout
        fig.update_layout(
            yaxis_title='Prize Amount ($)',
            xaxis_title='Number of Remaining Prizes',
            coloraxis_showscale=False,
            # Log scale for better visualization of wide prize ranges
            yaxis=dict(type='log')
        )

        return fig
