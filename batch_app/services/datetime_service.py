from datetime import datetime, timedelta


class DatetimeService:
    def get_today(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def get_yesterday(self) -> str:
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
