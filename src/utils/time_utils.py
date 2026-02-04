"""
Market Hours Utility
Handles US market trading hours and timezone conversions.
"""
from datetime import time, datetime, date
from typing import Optional
import pytz


class MarketHours:
    """
    US Stock Market trading hours utility.
    
    Regular trading hours: 9:30 AM - 4:00 PM Eastern Time
    Pre-market: 4:00 AM - 9:30 AM ET
    After-hours: 4:00 PM - 8:00 PM ET
    """
    
    ET = pytz.timezone("US/Eastern")
    UTC = pytz.UTC
    
    # Regular trading hours
    MARKET_OPEN = time(9, 30)
    MARKET_CLOSE = time(16, 0)
    
    # Extended hours
    PRE_MARKET_OPEN = time(4, 0)
    AFTER_HOURS_CLOSE = time(20, 0)
    
    # 2024-2025 US market holidays
    HOLIDAYS = [
        date(2024, 1, 1),    # New Year's Day
        date(2024, 1, 15),   # MLK Day
        date(2024, 2, 19),   # Presidents Day
        date(2024, 3, 29),   # Good Friday
        date(2024, 5, 27),   # Memorial Day
        date(2024, 6, 19),   # Juneteenth
        date(2024, 7, 4),    # Independence Day
        date(2024, 9, 2),    # Labor Day
        date(2024, 11, 28),  # Thanksgiving
        date(2024, 12, 25),  # Christmas
        date(2025, 1, 1),    # New Year's Day
        date(2025, 1, 20),   # MLK Day
        date(2025, 2, 17),   # Presidents Day
        date(2025, 4, 18),   # Good Friday
        date(2025, 5, 26),   # Memorial Day
        date(2025, 6, 19),   # Juneteenth
        date(2025, 7, 4),    # Independence Day
        date(2025, 9, 1),    # Labor Day
        date(2025, 11, 27),  # Thanksgiving
        date(2025, 12, 25),  # Christmas
    ]
    
    @classmethod
    def now_et(cls) -> datetime:
        """Get current time in Eastern timezone."""
        return datetime.now(cls.ET)
    
    @classmethod
    def is_weekend(cls, dt: Optional[datetime] = None) -> bool:
        """Check if given datetime is a weekend."""
        dt = dt or cls.now_et()
        return dt.weekday() >= 5  # Saturday = 5, Sunday = 6
    
    @classmethod
    def is_holiday(cls, dt: Optional[datetime] = None) -> bool:
        """Check if given datetime is a market holiday."""
        dt = dt or cls.now_et()
        return dt.date() in cls.HOLIDAYS
    
    @classmethod
    def is_regular_hours(cls, dt: Optional[datetime] = None) -> bool:
        """
        Check if market is in regular trading hours.
        
        Args:
            dt: Datetime to check (defaults to now)
            
        Returns:
            True if within 9:30 AM - 4:00 PM ET on a trading day
        """
        dt = dt or cls.now_et()
        
        if cls.is_weekend(dt) or cls.is_holiday(dt):
            return False
            
        current_time = dt.time()
        return cls.MARKET_OPEN <= current_time <= cls.MARKET_CLOSE
    
    @classmethod
    def is_extended_hours(cls, dt: Optional[datetime] = None) -> bool:
        """
        Check if market is in extended trading hours.
        
        Args:
            dt: Datetime to check (defaults to now)
            
        Returns:
            True if within pre-market or after-hours on a trading day
        """
        dt = dt or cls.now_et()
        
        if cls.is_weekend(dt) or cls.is_holiday(dt):
            return False
            
        current_time = dt.time()
        
        # Pre-market
        if cls.PRE_MARKET_OPEN <= current_time < cls.MARKET_OPEN:
            return True
            
        # After-hours
        if cls.MARKET_CLOSE < current_time <= cls.AFTER_HOURS_CLOSE:
            return True
            
        return False
    
    @classmethod
    def is_market_open(cls, include_extended: bool = False, dt: Optional[datetime] = None) -> bool:
        """
        Check if market is currently open.
        
        Args:
            include_extended: Include pre-market and after-hours
            dt: Datetime to check (defaults to now)
            
        Returns:
            True if market is open
        """
        if include_extended:
            return cls.is_regular_hours(dt) or cls.is_extended_hours(dt)
        return cls.is_regular_hours(dt)
    
    @classmethod
    def seconds_until_open(cls, dt: Optional[datetime] = None) -> int:
        """
        Calculate seconds until market opens.
        
        Args:
            dt: Datetime to calculate from (defaults to now)
            
        Returns:
            Seconds until market open, 0 if already open
        """
        dt = dt or cls.now_et()
        
        if cls.is_regular_hours(dt):
            return 0
            
        # Calculate next market open
        target_date = dt.date()
        
        # If after close or weekend/holiday, move to next trading day
        if dt.time() > cls.MARKET_CLOSE or cls.is_weekend(dt) or cls.is_holiday(dt):
            target_date = cls._next_trading_day(dt)
            
        next_open = datetime.combine(target_date, cls.MARKET_OPEN)
        next_open = cls.ET.localize(next_open)
        
        delta = next_open - dt
        return max(0, int(delta.total_seconds()))
    
    @classmethod
    def _next_trading_day(cls, dt: datetime) -> date:
        """Find the next trading day."""
        from datetime import timedelta
        
        next_day = dt.date() + timedelta(days=1)
        
        while next_day.weekday() >= 5 or next_day in cls.HOLIDAYS:
            next_day += timedelta(days=1)
            
        return next_day
    
    @classmethod
    def next_market_open(cls, dt: Optional[datetime] = None) -> datetime:
        """
        Get the datetime of the next market open.
        
        Args:
            dt: Datetime to calculate from (defaults to now)
            
        Returns:
            Datetime of the next market open in Eastern Time
        """
        dt = dt or cls.now_et()
        
        # If market is currently open, return current open time
        if cls.is_regular_hours(dt):
            return datetime.combine(dt.date(), cls.MARKET_OPEN, tzinfo=cls.ET)
        
        # Determine target date
        target_date = dt.date()
        
        # If before market open today and it's a trading day
        if (dt.time() < cls.MARKET_OPEN and 
            not cls.is_weekend(dt) and 
            not cls.is_holiday(dt)):
            pass  # Use today's date
        else:
            # After close or weekend/holiday, use next trading day
            target_date = cls._next_trading_day(dt)
        
        next_open = datetime.combine(target_date, cls.MARKET_OPEN)
        return cls.ET.localize(next_open)


# Example usage
if __name__ == "__main__":
    mh = MarketHours()
    
    print(f"Current ET time: {mh.now_et()}")
    print(f"Is weekend: {mh.is_weekend()}")
    print(f"Is holiday: {mh.is_holiday()}")
    print(f"Is regular hours: {mh.is_regular_hours()}")
    print(f"Is extended hours: {mh.is_extended_hours()}")
    print(f"Is market open: {mh.is_market_open()}")
    print(f"Seconds until open: {mh.seconds_until_open()}")
