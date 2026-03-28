from re import compile as re_compile
from re import escape

from numpy import busday_count
from pandas import NA, DateOffset, Series, Timestamp, isna
from pytz import timezone


class TransformationTools:
    """Class containing static methods for data transformation"""

    @staticmethod
    def replace_with_abbreviations(
        series: Series, abbr_dicts_list: list[dict[str, str]]
    ) -> Series:
        """
        Replace values in a pandas Series based on a list of abbreviation dictionaries.
        """
        merged_dict = {}
        for d in abbr_dicts_list:
            merged_dict.update(d)

        if not merged_dict:
            return series.fillna("").str.strip()

        pattern = r"\b(" + "|".join(map(escape, merged_dict.keys())) + r")\b"

        return (
            series.fillna("")
            .str.replace(pattern, lambda m: merged_dict[m.group(0)], regex=True)
            .str.strip()
        )

    @staticmethod
    def clean_and_titlecase(series: Series) -> Series:
        """Clean and title-case a pandas Series."""
        name_clean_regex = r"[^a-zA-ZÀ-ÖØ-öø-ÿ0-9\-\'\./ ]+"

        cleaned = (
            series.fillna("").str.strip().str.replace(name_clean_regex, "", regex=True)
        )

        def process_word(word):
            parts = word.split("-")
            processed_parts = [
                p if len(p) > 1 and p.isupper() else p.capitalize() for p in parts
            ]
            return "-".join(processed_parts)

        return cleaned.str.split().apply(
            lambda words: " ".join(process_word(w) for w in words)
        )

    @staticmethod
    def working_minutes(
        start: Timestamp, end: Timestamp, tz: str = "UTC", working_days: int = 5
    ) -> int:
        """
        Calculate the number of working (business) minutes between two timestamps,
        after normalizing both to the same timezone.

        For the first and last days (if they are weekdays), only the portion of the
        day between the given times is counted. If start and end fall on the same day
        and that day is a business day, the difference in minutes is returned.

        Parameters
        ----------
        start : pd.Timestamp
            The start timestamp (may be tz-naive or tz-aware).
        end : pd.Timestamp
            The end timestamp (may be tz-naive or tz-aware).
        tz : str, optional (default="UTC")
            Timezone in which to interpret both `start` and `end`. If they are
            naive, they will be localized; if they're already aware, they'll be
            converted to this zone.

        Returns
        -------
        int
            Total working-day minutes between `start` and `end` in the given timezone,
            or None if either input is NA, or 0 if `start > end`.

        """
        if isna(start) or isna(end):
            return None

        # Ensure both timestamps are timezone-aware in the same zone:
        target_tz = timezone(tz)

        if start.tzinfo is None:
            start = start.tz_localize(
                target_tz, ambiguous=True, nonexistent="shift_forward"
            )
        else:
            start = start.tz_convert(target_tz)

        if end.tzinfo is None:
            end = end.tz_localize(target_tz, ambiguous=True, nonexistent="shift_forward")
        else:
            end = end.tz_convert(target_tz)

        if start > end:
            return 0

        # Normalize to midnight (i.e. get the date at 00:00:00)
        start_midnight = start.normalize()
        end_midnight = end.normalize()

        if start_midnight == end_midnight:
            return (
                int((end - start).total_seconds() / 60)
                if start.weekday() < working_days
                else 0
            )

        total_minutes = 0

        if start.weekday() < working_days:
            end_of_start_day = start_midnight + DateOffset(days=1)
            total_minutes += (end_of_start_day - start).total_seconds() / 60

        if end.weekday() < working_days:
            total_minutes += (end - end_midnight).total_seconds() / 60

        full_business_days = busday_count(start_midnight.date(), end_midnight.date())

        if start.weekday() < working_days:
            full_business_days -= 1

        if full_business_days > 0:
            total_minutes += full_business_days * 1440

        return int(total_minutes)

    @staticmethod
    def clean_postal_codes(postal_codes: Series, country_codes: Series) -> Series:
        """
        Cleans and validates postal codes in a Series according to
        country-specific regex patterns.
        """

        # Precompiled country-specific postal code regex patterns
        postal_code_patterns = {
            "CA": re_compile(r"^[A-Z]\d[A-Z]\d[A-Z]\d$"),
            "GB": re_compile(r"^(GIR0AA|[A-Z]{1,2}\d{1,2}[A-Z]?\d[A-Z]{2})$"),
            "US": re_compile(r"^\d{5}(\d{4})?$"),
            "FR": re_compile(r"^\d{5}$"),
            "DE": re_compile(r"^\d{5}$"),
            "ES": re_compile(r"^\d{5}$"),
            "FI": re_compile(r"^\d{5}$"),
            "EE": re_compile(r"^\d{5}$"),
            "SE": re_compile(r"^\d{5}$"),
            "CZ": re_compile(r"^\d{5}$"),
            "PL": re_compile(r"^\d{5}$"),
            "NL": re_compile(r"^\d{4}[A-Z]{2}$"),
            "DK": re_compile(r"^\d{4}$"),
            "AT": re_compile(r"^\d{4}$"),
            "BE": re_compile(r"^\d{4}$"),
            "CH": re_compile(r"^\d{4}$"),
            "NO": re_compile(r"^\d{4}$"),
            "LU": re_compile(r"^\d{4}$"),
            "HU": re_compile(r"^\d{4}$"),
            "IE": re_compile(r"^[A-Z0-9]{7}$"),
            "IT": re_compile(r"^\d{5}$"),
            "MX": re_compile(r"^\d{5}$"),
            "ZA": re_compile(r"^\d{5}$"),
            "MY": re_compile(r"^\d{5}$"),
            "TH": re_compile(r"^\d{5}$"),
            "PT": re_compile(r"^\d{7}$"),
            "AU": re_compile(r"^\d{4}$"),
            "NZ": re_compile(r"^\d{4}$"),
            "PH": re_compile(r"^\d{4}$"),
            "JP": re_compile(r"^\d{7}$"),
            "KR": re_compile(r"^\d{5}$"),
            "SG": re_compile(r"^\d{6}$"),
            "GG": re_compile(r"^GY\d{1,2}[A-Z]{2}$"),
            "JE": re_compile(r"^JE\d{1,2}[A-Z]{2}$"),
            "IM": re_compile(r"^IM\d{1,2}[A-Z]{2}$"),
            "IS": re_compile(r"^\d{3}$"),
        }

        # Standardize postal codes: remove non-alphanumeric characters,
        # convert to uppercase, and trim whitespace
        postal_codes = (
            postal_codes.astype(str)
            .str.replace(r"[^0-9A-Za-z]", "", regex=True)
            .str.upper()
            .str.strip()
        )

        for country_code, pattern in postal_code_patterns.items():
            mask = country_codes == country_code
            postal_codes.loc[mask & ~postal_codes.str.match(pattern, na=False)] = NA

        return postal_codes
