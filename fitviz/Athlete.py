import datetime
from collections import namedtuple
from dataclasses import dataclass
from typing import Optional, List

import numpy as np
import pandas as pd


@dataclass
class Zone:
    name: str
    number: str
    rpe: float
    ftp_perc_bound: float
    lthr_perc_bound: Optional[float]


HR_ZONE_PCT = {1: 0.7, 2: 0.87, 3: 0.95, 4: 1, 4.5: 1.05}
PWR_ZONE_META: List[Zone] = [
    Zone("Active Recovery", "1", 2.5, 0.55, 0.7),
    Zone("Endurance", "2", 4.5, 0.75, 0.87),
    Zone("Tempo", "3", 6, 0.91, 0.95),
    Zone("Sub Threshold", "4a", 7, 1.0, 1.0),
    Zone("Supra Threshold", "4b", 8.0, 1.10, 1.05),
    Zone("VO2 Max", "5", 9.0, 1.35, np.inf),
    Zone("AC/NM", "6", 10.0, np.inf, None),
]


@dataclass
class Athlete:
    database_root: str
    name: str = "John Donich"
    ftp: Optional[float] = None
    map: Optional[float] = None
    ac: Optional[float] = None
    nm: Optional[float] = None
    lthr: Optional[float] = None
    test_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None

    def __str__(self):
        return self.get_zones_str()

    def four_dp(self) -> pd.DataFrame:
        return pd.DataFrame(
            [("ftp", self.ftp), ("map", self.map), ("ac", self.ac), ("nm", self.nm)],
            columns=("metric", "power"),
        )

    def get_hr_zones(self):
        assert self.lthr is not None
        return [self.lthr * pct for pct in HR_ZONE_PCT.values()]

    def get_hr_zone(self, hr: float) -> Optional[float]:
        zone = None
        if self.lthr is not None:
            hr_norm = hr / self.lthr
            if hr_norm < HR_ZONE_PCT[1]:
                zone = 1.0
            elif hr_norm < HR_ZONE_PCT[2]:
                zone = 2.0
            elif hr_norm < HR_ZONE_PCT[3]:
                zone = 3.0
            elif hr_norm < HR_ZONE_PCT[4]:
                zone = 4.0
            elif hr_norm < HR_ZONE_PCT[4.5]:
                zone = 4.5
            else:
                zone = 5.0
        return zone

    def get_power_zone(self, pwr: float) -> float:
        zone = 0.0
        if self.ftp is not None:
            pwr_norm = pwr / self.ftp
            if pwr_norm < 0.55:
                zone = 1.0
            elif pwr_norm < 0.75:
                zone = 2.0
            elif pwr_norm < 0.91:
                zone = 3.0
            elif pwr_norm < 1.0:
                zone = 4.0
            elif pwr_norm < 1.1:
                zone = 4.5
            elif self.map is not None and (pwr / self.map) < 1.25:
                zone = 5.0
            elif pwr_norm < 1.35:
                zone = 5.0
            elif self.nm is not None and (pwr / self.nm) > 0.75:
                zone = 7.0
            else:
                zone = 6.0
        return zone

    def get_zones_str(self):
        return self.get_zones_df().to_markdown()

    def get_zones_df(self) -> pd.DataFrame:
        assert self.ftp is not None
        assert self.lthr is not None
        pwr_lower_bound = 0
        hr_lower_bound = 0
        columns = (
            "zone_name",
            "zone_number",
            "low_power",
            "high_power",
            "low_hr",
            "high_hr",
        )
        data = []
        for zone in PWR_ZONE_META:
            pwr_upper_bound = np.round(self.ftp * zone.ftp_perc_bound)
            if zone.lthr_perc_bound is not None:
                hr_upper_bound = np.round(self.lthr * zone.lthr_perc_bound)
            else:
                hr_lower_bound = np.nan
                hr_upper_bound = np.nan

            data.append(
                (
                    zone.name,
                    zone.number,
                    pwr_lower_bound,
                    pwr_upper_bound,
                    hr_lower_bound,
                    hr_upper_bound,
                )
            )

            pwr_lower_bound = pwr_upper_bound + 1
            hr_lower_bound = hr_upper_bound + 1 if hr_upper_bound is not None else None

        return pd.DataFrame(data, columns=columns)
