import datetime
from dataclasses import dataclass
from typing import Optional, List

import pandas as pd

HR_ZONE_PCT = {1: 0.7, 2: 0.87, 3: 0.95, 4: 1, 4.5: 1.05}
PWR_ZONE_PCT = {1: 0.55, 2: 0.75, 3: 0.91, 4: 1, 4.5: 1.10, 5: 1.35}


@dataclass
class Athlete:
    name: str = "John Donich"
    ftp: Optional[float] = None
    map: Optional[float] = None
    ac: Optional[float] = None
    nm: Optional[float] = None
    lthr: Optional[float] = None
    test_date: Optional[datetime.date] = None
    end_date: Optional[datetime.date] = None

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

    def get_basic_pwr_zone(self):
        assert self.ftp is not None
        return [self.ftp * pct for pct in PWR_ZONE_PCT.values()]
