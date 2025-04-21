import numpy as np


def format_currency(value):
    """
    Format a numeric value as currency with comma separators

    Parameters:
    -----------
    value : float
        The numeric value to format

    Returns:
    --------
    str
        Formatted currency string with comma separators
    """
    # Handle NaN values
    if np.isnan(value):
        return "$0.00"

    # Convert to float if needed
    try:
        value = float(value)
    except:
        # If conversion fails, return original value with $ sign
        return f"${value}"

    # Format with comma separator, no cents
    return f"${value:,.0f}"


def calculate_probability(remaining, total):
    """
    Calculate probability of winning

    Parameters:
    -----------
    remaining : int
        Number of remaining prizes
    total : int
        Total number of tickets

    Returns:
    --------
    float
        Probability value between 0 and 1
    """
    if total <= 0 or remaining <= 0:
        return 0

    return remaining / total


def calculate_expected_value(probability, prize_amount, ticket_price):
    """
    Calculate expected value of a lottery ticket

    Parameters:
    -----------
    probability : float
        Probability of winning (between 0 and 1)
    prize_amount : float
        Prize amount in dollars
    ticket_price : float
        Ticket price in dollars

    Returns:
    --------
    float
        Expected value in dollars
    """
    if probability <= 0 or np.isnan(probability) or np.isnan(prize_amount) or np.isnan(ticket_price):
        return -ticket_price

    return (probability * prize_amount) - ticket_price


def parse_date_range(date_range_str):
    """
    Parse a date range string into a tuple of datetime objects

    Parameters:
    -----------
    date_range_str : str
        String representation of date range (e.g., "2022-01-01 to 2022-12-31")

    Returns:
    --------
    tuple
        Tuple of (start_date, end_date) as datetime objects
    """
    from datetime import datetime

    if not date_range_str or 'to' not in date_range_str:
        return None

    try:
        parts = date_range_str.split('to')
        start_date = datetime.strptime(parts[0].strip(), '%Y-%m-%d')
        end_date = datetime.strptime(parts[1].strip(), '%Y-%m-%d')
        return (start_date, end_date)
    except Exception:
        return None
