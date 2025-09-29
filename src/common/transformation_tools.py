from re import compile, escape
from typing import Dict, List

import pytz
from numpy import busday_count
from pandas import NA, DateOffset, Series, Timestamp, isna


class TransformationTools:
    """Class containing static methods for data transformation"""

    @staticmethod
    def replace_with_abbreviations(
        series: Series, abbr_dicts_list: List[Dict[str, str]]
    ) -> Series:
        """Replace values in a pandas Series based on a list of abbreviation dictionaries."""
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
        NAME_CLEAN_REGEX = r"[^a-zA-ZÀ-ÖØ-öø-ÿ0-9\-\'\./ ]+"

        cleaned = (
            series.fillna("").str.strip().str.replace(NAME_CLEAN_REGEX, "", regex=True)
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
    def working_minutes(start: Timestamp, end: Timestamp, tz: str = "UTC") -> int:
        """
        Calculate the number of working (business) minutes between two timestamps, after normalizing both to the same timezone.

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

        # Ensure both timestamps are timezone‐aware in the same zone:
        target_tz = pytz.timezone(tz)

        if start.tzinfo is None:
            start = start.tz_localize(
                target_tz, ambiguous=True, nonexistent="shift_forward"
            )
        else:
            start = start.tz_convert(target_tz)

        if end.tzinfo is None:
            end = end.tz_localize(
                target_tz, ambiguous=True, nonexistent="shift_forward"
            )
        else:
            end = end.tz_convert(target_tz)

        if start > end:
            return 0

        # Normalize to midnight (i.e. get the date at 00:00:00)
        start_midnight = start.normalize()
        end_midnight = end.normalize()

        if start_midnight == end_midnight:
            return int((end - start).total_seconds() / 60) if start.weekday() < 5 else 0

        total_minutes = 0

        if start.weekday() < 5:
            end_of_start_day = start_midnight + DateOffset(days=1)
            total_minutes += (end_of_start_day - start).total_seconds() / 60

        if end.weekday() < 5:
            total_minutes += (end - end_midnight).total_seconds() / 60

        full_business_days = busday_count(start_midnight.date(), end_midnight.date())

        if start.weekday() < 5:
            full_business_days -= 1

        if full_business_days > 0:
            total_minutes += full_business_days * 1440

        return int(total_minutes)

    @staticmethod
    def clean_postal_codes(postal_codes: Series, country_codes: Series) -> Series:
        """
        Cleans and validates postal codes in a Series according to country-specific regex patterns.
        """

        # Precompiled country-specific postal code regex patterns
        postal_code_patterns = {
            "CA": compile(r"^[A-Z]\d[A-Z]\d[A-Z]\d$"),
            "GB": compile(r"^(GIR0AA|[A-Z]{1,2}\d{1,2}[A-Z]?\d[A-Z]{2})$"),
            "US": compile(r"^\d{5}(\d{4})?$"),
            "FR": compile(r"^\d{5}$"),
            "DE": compile(r"^\d{5}$"),
            "ES": compile(r"^\d{5}$"),
            "FI": compile(r"^\d{5}$"),
            "EE": compile(r"^\d{5}$"),
            "SE": compile(r"^\d{5}$"),
            "CZ": compile(r"^\d{5}$"),
            "PL": compile(r"^\d{5}$"),
            "NL": compile(r"^\d{4}[A-Z]{2}$"),
            "DK": compile(r"^\d{4}$"),
            "AT": compile(r"^\d{4}$"),
            "BE": compile(r"^\d{4}$"),
            "CH": compile(r"^\d{4}$"),
            "NO": compile(r"^\d{4}$"),
            "LU": compile(r"^\d{4}$"),
            "HU": compile(r"^\d{4}$"),
            "IE": compile(r"^[A-Z0-9]{7}$"),
            "IT": compile(r"^\d{5}$"),
            "MX": compile(r"^\d{5}$"),
            "ZA": compile(r"^\d{5}$"),
            "MY": compile(r"^\d{5}$"),
            "TH": compile(r"^\d{5}$"),
            "PT": compile(r"^\d{7}$"),
            "AU": compile(r"^\d{4}$"),
            "NZ": compile(r"^\d{4}$"),
            "PH": compile(r"^\d{4}$"),
            "JP": compile(r"^\d{7}$"),
            "KR": compile(r"^\d{5}$"),
            "SG": compile(r"^\d{6}$"),
            "GG": compile(r"^GY\d{1,2}[A-Z]{2}$"),
            "JE": compile(r"^JE\d{1,2}[A-Z]{2}$"),
            "IM": compile(r"^IM\d{1,2}[A-Z]{2}$"),
            "IS": compile(r"^\d{3}$"),
        }

        # Standardize postal codes: remove non-alphanumeric characters, uppercase, and trim
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
