from datetime import datetime, timedelta


def get_next_midday_utc(current_time: datetime) -> datetime:
    """
    Calculate the next midday UTC from the current time.

    :param current_time: datetime, current UTC time
    :return: datetime, next midday UTC
    """
    # Create midday today
    midday_today = current_time.replace(hour=12, minute=0, second=0, microsecond=0)

    # If we're past midday today, get midday tomorrow
    if current_time >= midday_today:
        return midday_today + timedelta(days=1)
    else:
        return midday_today
