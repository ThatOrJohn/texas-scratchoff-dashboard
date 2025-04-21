import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

from neo4j_connector import Neo4jConnector
from data_processor import DataProcessor
from visualizations import Visualizations
from utils import format_currency, calculate_probability

# Get database connection credentials from environment variables (secrets)
NEO4J_URI = os.environ.get("NEO4J_URI", "")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "")

# Page configuration
st.set_page_config(
    page_title="Texas Lottery Scratchoff Data Dashboard",
    page_icon="🎰",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize session state for connection status
if 'connected' not in st.session_state:
    st.session_state.connected = False

# Initialize session state for data
if 'lottery_data' not in st.session_state:
    st.session_state.lottery_data = pd.DataFrame()

if 'games_data' not in st.session_state:
    st.session_state.games_data = pd.DataFrame()

if 'prizes_data' not in st.session_state:
    st.session_state.prizes_data = pd.DataFrame()


def main():
    # Close any existing database connection when the app is restarted
    if 'neo4j_connector' in st.session_state and hasattr(st.session_state.neo4j_connector, 'close'):
        try:
            st.session_state.neo4j_connector.close()
        except:
            pass  # Ignore errors during connection cleanup

    # Automatically connect to the database using the secrets
    if not st.session_state.connected:
        try:
            # Use the connection details from secrets
            connector = Neo4jConnector(
                NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
            st.session_state.neo4j_connector = connector

            # Check connection
            if connector.test_connection():
                st.session_state.connected = True

                # Load initial data
                data_processor = DataProcessor(connector)
                st.session_state.games_data = data_processor.get_all_games()
                st.session_state.prizes_data = data_processor.get_all_prizes()
                st.session_state.lottery_data = data_processor.get_combined_data()
            else:
                st.error(
                    "Failed to connect to the database. Please check your connection settings.")
        except Exception as e:
            st.error(f"Connection error: {str(e)}")

    # Sidebar for filters
    with st.sidebar:
        # Title removed as requested

        # Only show filters if connected
        if st.session_state.connected and st.session_state.games_data is not None:
            st.subheader("Filters")

            # Store filter settings in session state to detect changes
            if 'prev_filter_price_range' not in st.session_state:
                st.session_state.prev_filter_price_range = (1, 100)

            # Ticket Price filter from 1 to 100
            selected_ticket_price_range = st.slider("Ticket Price ($)",
                                                    1, 100, (1, 100),
                                                    help="Filter games by ticket price")

            # Removed "Games ending soon" filter as requested
            # Default to "include all games" for backward compatibility
            st.session_state.ending_filter = "include"

            # Check if filters have changed
            filters_changed = (selected_ticket_price_range !=
                               st.session_state.prev_filter_price_range)

            # Apply filters automatically when they change
            if filters_changed:
                data_processor = DataProcessor(
                    st.session_state.neo4j_connector)

                # Apply ticket price filter only (no game filter)
                min_ticket_price, max_ticket_price = selected_ticket_price_range

                # Get filtered data
                st.session_state.lottery_data = data_processor.get_filtered_data(
                    game_id=None,  # No game filter
                    min_ticket_price=min_ticket_price,
                    max_ticket_price=max_ticket_price,
                    # Use the ending filter from session state
                    ending_filter=st.session_state.ending_filter
                )

                # Update the previous filter value
                st.session_state.prev_filter_price_range = selected_ticket_price_range

    # Main dashboard area
    st.title("Texas Lottery Scratchoff Analysis Dashboard")

    if not st.session_state.connected:
        st.info(
            "Please connect to the Neo4j database using the sidebar to view the dashboard.")

        # Show sample dashboard structure
        st.subheader("Dashboard Preview (Connect to see real data)")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Game Overview")
            st.text("Displays game information and statistics")
            st.empty()  # Placeholder for chart

        with col2:
            st.markdown("### Prize Distribution")
            st.text("Shows prize levels and availability")
            st.empty()  # Placeholder for chart

        st.markdown("### Detailed Game Analysis")
        st.text("Provides in-depth analysis of selected games")
        st.empty()  # Placeholder for detailed table

    else:
        # Data is loaded, show the dashboard
        if st.session_state.lottery_data is not None:
            # Create visualization object
            viz = Visualizations(st.session_state.lottery_data)

            # Summary metrics (only Total Games and Games Ending Soon as requested)
            st.subheader("Dashboard Summary")

            # Add the data last updated info from the last_updated field if available
            if "last_updated" in st.session_state.games_data.columns:
                # Try to get the most recent last_updated value
                date_updated = None
                try:
                    # Convert to datetime if it's a string
                    if isinstance(st.session_state.games_data["last_updated"].iloc[0], str):
                        st.session_state.games_data["last_updated"] = pd.to_datetime(
                            st.session_state.games_data["last_updated"], errors="coerce"
                        )

                    # Get the most recent date
                    date_updated = st.session_state.games_data["last_updated"].max(
                    )

                    # Format the date
                    if pd.notna(date_updated):
                        formatted_date = date_updated.strftime("%B %d, %Y")
                        st.info(f"Data Last Updated: {formatted_date}")
                except Exception as e:
                    # If there's an error, just skip showing the date
                    st.info("Data Last Updated: Information not available")
            else:
                st.info("Data Last Updated: Information not available")

            metrics_col1, metrics_col2, metrics_col3 = st.columns(3)

            with metrics_col1:
                if "game_id" in st.session_state.games_data.columns:
                    total_games = len(
                        st.session_state.games_data["game_id"].unique())
                    st.metric("Total Games", total_games)
                else:
                    st.metric("Total Games", 0)

            with metrics_col2:
                # Games ending soon: count of Game nodes with a non-null/non-empty game_close_date
                if "game_close_date" in st.session_state.games_data.columns:
                    # Count games where game_close_date is not NULL AND not empty string
                    games_ending_soon_data = st.session_state.games_data[
                        st.session_state.games_data["game_close_date"].notna() &
                        (st.session_state.games_data["game_close_date"] != "")
                    ]
                    games_ending_soon_count = games_ending_soon_data.shape[0]
                    st.metric("Games Ending Soon", games_ending_soon_count)
                else:
                    st.metric("Games Ending Soon", 0)

            with metrics_col3:
                # Games to Avoid (90%+ top prizes claimed)
                try:
                    # Get the list of games to avoid from Neo4j
                    data_processor = DataProcessor(
                        st.session_state.neo4j_connector)
                    games_to_avoid_df = data_processor.get_games_to_avoid()
                    games_to_avoid_count = len(
                        games_to_avoid_df) if not games_to_avoid_df.empty else 0
                    st.metric("Games to Avoid", games_to_avoid_count,
                              help="Games where 90% or more of the top prizes have been claimed")

                    # Store in session state for later use
                    st.session_state.games_to_avoid = games_to_avoid_df
                except Exception as e:
                    st.metric("Games to Avoid", "Error",
                              help=f"Failed to fetch games to avoid: {str(e)}")
                    st.session_state.games_to_avoid = pd.DataFrame()

            # Timeline section removed as requested

            # Add breakdown of game counts by ticket price
            st.subheader("Games by Ticket Price")
            if "ticket_price" in st.session_state.games_data.columns:
                # Group games by ticket price and count
                ticket_price_counts = st.session_state.games_data.groupby(
                    "ticket_price").size().reset_index(name="count")
                ticket_price_counts = ticket_price_counts.sort_values(
                    by="ticket_price")

                # Format ticket prices as currency
                ticket_price_counts["ticket_price_formatted"] = ticket_price_counts["ticket_price"].apply(
                    # No cents display for cleaner labels
                    lambda x: f"${float(x):.0f}"
                )

                # Create a bar chart using plotly
                fig = px.bar(
                    ticket_price_counts,
                    x="ticket_price",
                    y="count",
                    labels={
                        "ticket_price": "Ticket Price ($)", "count": "Number of Games"},
                    title="Number of Games by Ticket Price",
                    text="count"  # Display count values on bars
                )

                # Completely disable hover information
                fig.update_layout(hovermode=False)

                # Set text position for count values
                fig.update_traces(
                    textposition='auto'  # Automatically position text
                )

                # Configure x-axis to show specific ticket prices (1, 2, 5, 10, 20, 30, 50, 100)
                # instead of an evenly distributed scale
                fig.update_xaxes(
                    type='category',  # Use categorical axis for correct spacing
                    # Only show actual price points
                    tickvals=ticket_price_counts["ticket_price"].tolist(),
                    # Format as currency
                    ticktext=ticket_price_counts["ticket_price_formatted"].tolist(
                    )
                )

                # Improve visual style
                fig.update_layout(
                    bargap=0.2,  # Add some gap between bars
                    xaxis_title="Ticket Price ($)",
                    yaxis_title="Number of Games"
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Ticket price data not available")

            # Prize Availability section removed as requested

            # Display Games to Avoid if there are any
            if hasattr(st.session_state, 'games_to_avoid') and not st.session_state.games_to_avoid.empty:
                st.subheader("Games to Avoid (90%+ Top Prizes Claimed)")

                # Format the data for display
                display_df = st.session_state.games_to_avoid.copy()

                # Format numeric columns
                if 'prize_level' in display_df.columns:
                    display_df['Top Prize'] = display_df['prize_level'].apply(
                        format_currency)

                if 'ticket_price' in display_df.columns:
                    display_df['Ticket Price'] = display_df['ticket_price'].apply(
                        lambda x: f"${float(x):.2f}")

                if 'claim_rate' in display_df.columns:
                    display_df['Claim Rate'] = display_df['claim_rate'].apply(
                        lambda x: f"{float(x)*100:.1f}%")

                # Use formatted game name if available, otherwise format it here
                if 'formatted_game_name' in display_df.columns:
                    display_df = display_df.rename(
                        columns={'formatted_game_name': 'Game'})
                elif all(col in display_df.columns for col in ['game_name', 'game_id']):
                    display_df['Game'] = display_df.apply(
                        lambda row: f"{row['game_name']} ({row['game_id']})", axis=1
                    )
                elif 'game_name' in display_df.columns:
                    display_df = display_df.rename(
                        columns={'game_name': 'Game'})

                # Select columns for display
                display_cols = ['Game', 'Top Prize',
                                'Ticket Price', 'Claim Rate']

                # Keep only the columns we want to display that exist
                cols_to_display = [
                    col for col in display_cols if col in display_df.columns]

                # Display the table
                st.dataframe(display_df[cols_to_display],
                             hide_index=True, use_container_width=True)

            # Default to include all games in case sidebar filter isn't accessible
            ending_filter = "include"

            # Try to get the sidebar filter value via session state
            if 'ending_filter' in st.session_state:
                ending_filter = st.session_state.ending_filter

            # Add a game detail section for drill-down functionality
            st.subheader("Game Detail View")
            st.info(
                "Select a specific game below to see detailed information about its prizes and distribution.")

            # Dropdown to select a specific game to view details
            if 'formatted_game_name' in st.session_state.lottery_data.columns:
                # Use formatted game names if available (these have the game number in parentheses)
                unique_games = sorted(
                    st.session_state.lottery_data["formatted_game_name"].unique())
                selected_detail_game = st.selectbox("Select a Game for Details", [
                                                    ""] + unique_games, index=0)
            elif "game_name" in st.session_state.lottery_data.columns:
                # If no formatted names available, use regular game names
                unique_games = sorted(
                    st.session_state.lottery_data["game_name"].unique())
                selected_detail_game = st.selectbox("Select a Game for Details", [
                                                    ""] + unique_games, index=0)

            # Only proceed if a game was selected from the dropdown
            if selected_detail_game:
                game_detail_data = None  # Initialize variable to store game details
                game_id = None  # Initialize game ID

                # First try to extract the game number from the formatted name
                # Formatted game names should follow the pattern "Game Name (game_number)"
                import re
                game_number_match = re.search(
                    r'\((\d+)\)$', selected_detail_game)
                if game_number_match:
                    game_id = game_number_match.group(1)

                # If we have a Neo4j connector and a game ID, get the details from Neo4j
                if 'neo4j_connector' in st.session_state and game_id:
                    try:
                        # Fetch detailed prize information for this game
                        prize_details = st.session_state.neo4j_connector.get_game_prize_details(
                            game_id)
                        game_detail_data = pd.DataFrame(prize_details)
                    except Exception as e:
                        st.error(f"Error fetching game details: {str(e)}")

                # If we couldn't get data from Neo4j or no game_id was found, fall back to filtering the dataframe
                if game_detail_data is None or (hasattr(game_detail_data, 'empty') and game_detail_data.empty):
                    # First try to use the game ID to filter if available
                    if game_id and "game_id" in st.session_state.lottery_data.columns:
                        game_detail_data = st.session_state.lottery_data[
                            st.session_state.lottery_data["game_id"] == game_id
                        ]
                    # Otherwise filter by name
                    elif 'formatted_game_name' in st.session_state.lottery_data.columns:
                        game_detail_data = st.session_state.lottery_data[
                            st.session_state.lottery_data["formatted_game_name"] == selected_detail_game
                        ]
                    else:
                        game_detail_data = st.session_state.lottery_data[
                            st.session_state.lottery_data["game_name"] == selected_detail_game
                        ]

                # Display the data if we have it
                if game_detail_data is not None and not (hasattr(game_detail_data, 'empty') and game_detail_data.empty):

                    # Display basic game information
                    game_info_cols = st.columns([1, 2])

                    # Left column for metrics
                    with game_info_cols[0]:
                        if "ticket_price" in game_detail_data.columns:
                            st.metric("Ticket Price", format_currency(
                                game_detail_data["ticket_price"].iloc[0]))

                        total_prizes = 0
                        prizes_claimed = 0

                        # Get total prizes and prizes claimed
                        if "total_prizes" in game_detail_data.columns:
                            total_prizes = game_detail_data["total_prizes"].iloc[0]
                            st.metric("Total Prizes", f"{total_prizes:,}")
                        elif "total_count" in game_detail_data.columns:
                            total_prizes = game_detail_data["total_count"].iloc[0]
                            st.metric("Total Prizes", f"{total_prizes:,}")

                        if "prizes_claimed" in game_detail_data.columns:
                            prizes_claimed = game_detail_data["prizes_claimed"].iloc[0]
                            st.metric("Prizes Claimed", f"{prizes_claimed:,}")
                        elif "claimed_count" in game_detail_data.columns:
                            prizes_claimed = game_detail_data["claimed_count"].iloc[0]
                            st.metric("Prizes Claimed", f"{prizes_claimed:,}")

                    # Right column for pie chart
                    with game_info_cols[1]:
                        if total_prizes > 0:
                            # Calculate prizes remaining
                            prizes_remaining = total_prizes - prizes_claimed
                            if prizes_remaining < 0:  # Handle data inconsistency
                                prizes_remaining = 0

                            # Create pie chart data
                            pie_data = pd.DataFrame({
                                'Status': ['Prizes Claimed', 'Prizes Remaining'],
                                'Count': [prizes_claimed, prizes_remaining]
                            })

                            # Create pie chart
                            fig = px.pie(
                                pie_data,
                                values='Count',
                                names='Status',
                                title=f"Prize Distribution for {selected_detail_game}",
                                color='Status',
                                color_discrete_map={
                                    'Prizes Claimed': 'red', 'Prizes Remaining': 'green'}
                            )

                            # Remove hover text but keep text inside pie chart sections
                            fig.update_traces(
                                textposition='inside',
                                textinfo='percent+value',
                                hoverinfo='skip'  # Skip hover info completely
                            )

                            # Display the pie chart
                            st.plotly_chart(fig, use_container_width=True)

                    # Show a breakdown of prize levels by Detail nodes
                    st.markdown("#### Prize Breakdown")

                    # Process game detail data
                    if "prize_level" in game_detail_data.columns:
                        # Make sure numeric columns are numeric
                        numeric_columns = [
                            "prize_level", "detail_total_prizes", "detail_prizes_claimed"]
                        for col in numeric_columns:
                            if col in game_detail_data.columns:
                                game_detail_data[col] = pd.to_numeric(
                                    game_detail_data[col], errors="coerce")

                        # Sort by prize level (highest value prizes first)
                        if "prize_level" in game_detail_data.columns:
                            game_detail_data = game_detail_data.sort_values(
                                "prize_level", ascending=False)

                        # Prepare prize breakdown data
                        prize_breakdown = pd.DataFrame()

                        # Prize Amount column (from prize_level)
                        if "prize_level" in game_detail_data.columns:
                            prize_breakdown["Prize Amount"] = game_detail_data["prize_level"].copy(
                            )
                            # Format as currency
                            prize_breakdown["Prize Amount"] = prize_breakdown["Prize Amount"].apply(
                                format_currency)

                        # Total Prizes column (from detail_total_prizes or detail_total_count)
                        if "detail_total_prizes" in game_detail_data.columns:
                            prize_breakdown["Total Prizes"] = game_detail_data["detail_total_prizes"]
                        elif "detail_total_count" in game_detail_data.columns:
                            prize_breakdown["Total Prizes"] = game_detail_data["detail_total_count"]

                        # Prizes Claimed column (from detail_prizes_claimed or detail_claimed_count)
                        if "detail_prizes_claimed" in game_detail_data.columns:
                            prize_breakdown["Prizes Claimed"] = game_detail_data["detail_prizes_claimed"]
                        elif "detail_claimed_count" in game_detail_data.columns:
                            prize_breakdown["Prizes Claimed"] = game_detail_data["detail_claimed_count"]

                        # Calculate remaining prizes
                        if "Total Prizes" in prize_breakdown.columns:
                            # First, handle missing or null values in Prizes Claimed
                            if "Prizes Claimed" not in prize_breakdown.columns:
                                prize_breakdown["Prizes Claimed"] = 0
                            else:
                                # Replace NA/null values with 0
                                prize_breakdown["Prizes Claimed"] = prize_breakdown["Prizes Claimed"].fillna(
                                    0)
                                # Make sure values are numeric
                                prize_breakdown["Prizes Claimed"] = pd.to_numeric(
                                    prize_breakdown["Prizes Claimed"], errors='coerce').fillna(0)

                            # Now calculate remaining prizes
                            prize_breakdown["Prizes Remaining"] = prize_breakdown["Total Prizes"] - \
                                prize_breakdown["Prizes Claimed"]

                            # Format numeric columns with comma separators
                            prize_breakdown["Total Prizes"] = prize_breakdown["Total Prizes"].apply(
                                lambda x: f"{int(x):,}")
                            prize_breakdown["Prizes Claimed"] = prize_breakdown["Prizes Claimed"].apply(
                                lambda x: f"{int(x):,}")
                            prize_breakdown["Prizes Remaining"] = prize_breakdown["Prizes Remaining"].apply(
                                lambda x: f"{int(x):,}")

                            # Calculate percent claimed as a numeric column first
                            # Handle division by zero by using numpy to set those cases to 0%
                            # Store original values before replacing with formatted ones
                            total_prizes_numeric = pd.to_numeric(
                                prize_breakdown["Total Prizes"].str.replace(',', ''), errors='coerce')
                            prizes_claimed_numeric = pd.to_numeric(
                                prize_breakdown["Prizes Claimed"].str.replace(',', ''), errors='coerce')

                            prize_breakdown["Percent Claimed Numeric"] = (
                                prizes_claimed_numeric / total_prizes_numeric) * 100
                            # Replace any NaN values (from division by zero) with 0
                            prize_breakdown["Percent Claimed Numeric"] = prize_breakdown["Percent Claimed Numeric"].fillna(
                                0)

                            # Format percent claimed for display
                            prize_breakdown["Percent Claimed"] = prize_breakdown["Percent Claimed Numeric"].apply(
                                lambda x: f"{x:.2f}%")

                            # Add color coding for the largest prize amount row if it exists
                            if len(prize_breakdown) > 0:
                                # Get the largest prize (first row)
                                top_prize = prize_breakdown.iloc[0]
                                percent_claimed = top_prize["Percent Claimed Numeric"]

                                # Create a message about the top prize status
                                top_prize_message = f"Top prize status: "
                                if percent_claimed <= 25:
                                    top_prize_message += f"🟢 Good! Only {percent_claimed:.2f}% claimed"
                                    top_prize_color = "green"
                                elif percent_claimed <= 75:
                                    top_prize_message += f"🟡 Moderate: {percent_claimed:.2f}% claimed"
                                    top_prize_color = "orange"
                                else:
                                    top_prize_message += f"🔴 Limited: {percent_claimed:.2f}% claimed"
                                    top_prize_color = "red"

                                # Display the top prize message with color
                                st.markdown(
                                    f"<p style='color:{top_prize_color};font-weight:bold'>{top_prize_message}</p>", unsafe_allow_html=True)

                            # Remove the numeric column before display
                            prize_breakdown = prize_breakdown.drop(
                                columns=["Percent Claimed Numeric"])

                            # Display the dataframe
                            st.dataframe(
                                prize_breakdown, use_container_width=True, hide_index=True)
                        else:
                            # If we don't have prize data, show the regular table
                            st.dataframe(
                                prize_breakdown, use_container_width=True, hide_index=True)

            # Add a clear visual separator
            st.markdown("---")

            # Bottom section with detailed table of all games
            # Make it very clear this is information for ALL games
            with st.expander("📊 ALL GAMES: Detailed Information Table", expanded=False):
                st.info(
                    "This table shows combined information for all games in the database. Use this to compare games side-by-side.")
                # Define what columns to show in the detailed table
                # Use formatted_game_name if available, otherwise use game_name
                name_col = "formatted_game_name" if "formatted_game_name" in st.session_state.lottery_data.columns else "game_name"

                # Add specific columns for the table display, including game_id/game_number
                id_col = None
                if "game_id" in st.session_state.lottery_data.columns:
                    id_col = "game_id"
                elif "game_number" in st.session_state.lottery_data.columns:
                    id_col = "game_number"

                # Create the column list with game number first, then game name
                detail_cols = []
                if id_col:
                    detail_cols.append(id_col)  # Game ID/Number column
                detail_cols.extend([name_col, "ticket_price", "total_prizes", "prizes_claimed",
                                    "percent_prizes_claimed", "expected_value"])

                # Format the data for display
                display_data = st.session_state.lottery_data.copy()

                # Calculate percent_prizes_claimed as requested: (prizes_claimed/total_prizes)*100
                # Handle missing or zero values in main game table too

                # Try to use the prizes_claimed and total_prizes fields if available
                if all(col in display_data.columns for col in ["prizes_claimed", "total_prizes"]):
                    # Ensure values are numeric and handle missing values
                    display_data["prizes_claimed"] = pd.to_numeric(
                        display_data["prizes_claimed"], errors='coerce').fillna(0)
                    display_data["total_prizes"] = pd.to_numeric(
                        display_data["total_prizes"], errors='coerce').fillna(0)

                    # Calculate percent
                    display_data["percent_prizes_claimed"] = (
                        display_data["prizes_claimed"] / display_data["total_prizes"]) * 100
                    # Replace any NaN with 0
                    display_data["percent_prizes_claimed"] = display_data["percent_prizes_claimed"].fillna(
                        0)
                    display_data["percent_prizes_claimed"] = display_data["percent_prizes_claimed"].apply(
                        lambda x: f"{x:.2f}%")

                    # Format prizes_claimed and total_prizes with comma separators
                    display_data["prizes_claimed"] = display_data["prizes_claimed"].apply(
                        lambda x: f"{int(x):,}")
                    display_data["total_prizes"] = display_data["total_prizes"].apply(
                        lambda x: f"{int(x):,}")

                # Fall back to claimed_count and total_count if needed
                elif all(col in display_data.columns for col in ["claimed_count", "total_count"]):
                    # Ensure values are numeric and handle missing values
                    display_data["claimed_count"] = pd.to_numeric(
                        display_data["claimed_count"], errors='coerce').fillna(0)
                    display_data["total_count"] = pd.to_numeric(
                        display_data["total_count"], errors='coerce').fillna(0)

                    # Calculate percent
                    display_data["percent_prizes_claimed"] = (
                        display_data["claimed_count"] / display_data["total_count"]) * 100
                    # Replace any NaN with 0
                    display_data["percent_prizes_claimed"] = display_data["percent_prizes_claimed"].fillna(
                        0)
                    display_data["percent_prizes_claimed"] = display_data["percent_prizes_claimed"].apply(
                        lambda x: f"{x:.2f}%")

                    # Format claimed_count and total_count with comma separators
                    display_data["claimed_count"] = display_data["claimed_count"].apply(
                        lambda x: f"{int(x):,}")
                    display_data["total_count"] = display_data["total_count"].apply(
                        lambda x: f"{int(x):,}")

                # Sort the data by ticket price (numerically) before formatting
                if "ticket_price" in display_data.columns:
                    # Convert to numeric first to ensure proper sorting
                    display_data["ticket_price"] = pd.to_numeric(
                        display_data["ticket_price"], errors="coerce")
                    # Now sort the dataframe by ticket price
                    display_data = display_data.sort_values("ticket_price")
                    # Now format as currency for display
                    display_data["ticket_price"] = display_data["ticket_price"].apply(
                        format_currency)

                if "expected_value" in display_data.columns:
                    display_data["expected_value"] = display_data["expected_value"].apply(
                        format_currency)

                # Make sure available_cols is defined
                available_cols = [
                    col for col in detail_cols if col in display_data.columns]

                # Make sure name column is a string for display
                if name_col in display_data.columns:
                    # Convert the name column to strings (if not already)
                    display_data[name_col] = display_data[name_col].astype(str)

                    # Rename the column to "Game Name" for display
                    display_data = display_data.rename(
                        columns={name_col: "Game Name"})

                    # Update the column name in available_cols
                    if name_col in available_cols:
                        index = available_cols.index(name_col)
                        available_cols[index] = "Game Name"

                # Also make sure game ID/number column is properly formatted
                if id_col and id_col in display_data.columns:
                    # Convert the ID column to strings for display
                    display_data[id_col] = display_data[id_col].astype(str)

                    # Rename the column to a more user-friendly name for display
                    if id_col == "game_id":
                        display_data = display_data.rename(
                            columns={"game_id": "Game Number"})
                    elif id_col == "game_number":
                        display_data = display_data.rename(
                            columns={"game_number": "Game Number"})

                    # Update the column name in the available_cols list
                    if id_col in available_cols:
                        index = available_cols.index(id_col)
                        available_cols[index] = "Game Number"

                # Rename other columns to be more user-friendly before display
                column_renames = {
                    "ticket_price": "Ticket Price",
                    "total_prizes": "Total Prizes",
                    "prizes_claimed": "Prizes Claimed",
                    "percent_prizes_claimed": "Percent Claimed",
                    "expected_value": "Expected Value"
                }

                # Apply the renames to both the DataFrame and the column list
                for old_name, new_name in column_renames.items():
                    if old_name in display_data.columns:
                        display_data = display_data.rename(
                            columns={old_name: new_name})
                    if old_name in available_cols:
                        index = available_cols.index(old_name)
                        available_cols[index] = new_name

                # Show the detailed table with hidden row numbers
                st.dataframe(display_data[available_cols],
                             use_container_width=True, hide_index=True)

                # Add download button for the data
                st.download_button(
                    label="Download Data as CSV",
                    data=display_data[available_cols].to_csv(
                        index=False).encode('utf-8'),
                    file_name=f"texas_lottery_data_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )


def add_kofi_widget():
    """
    Add Ko-fi donation button to the app using a direct link and image
    """
    kofi_html = """
    <div style="text-align: center; margin-top: 20px; margin-bottom: 20px;">
        <a href='https://ko-fi.com/Y8Y51DHIYY' target='_blank'>
            <img height='36' style='border:0px;height:36px;' 
                src='https://storage.ko-fi.com/cdn/kofi6.png?v=6' border='0' 
                alt='Buy Me a Coffee at ko-fi.com' />
        </a>
    </div>
    """
    # Use components.v1.html to add the Ko-fi button
    st.components.v1.html(kofi_html, height=70, scrolling=False)


if __name__ == "__main__":
    main()
    # Add the Ko-fi widget at the end
    add_kofi_widget()
