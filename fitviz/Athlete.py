import datetime
from dataclasses import dataclass
from typing import Optional

import pandas as pd


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

    def get_hr_zone(self, hr: float) -> float:
        zone = 0.0
        if self.lthr is not None:
            hr_norm = hr / self.lthr
            if hr_norm < 0.7:
                zone = 1.0
            elif hr_norm < 0.87:
                zone = 2.0
            elif hr_norm < 0.95:
                zone = 3.0
            elif hr_norm < 1:
                zone = 4.0
            elif hr_norm < 1.05:
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
