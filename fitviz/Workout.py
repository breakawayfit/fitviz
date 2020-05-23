import datetime
from typing import Optional

import altair as alt
import pandas as pd

from .Athlete import Athlete
from .PdxFile import PwxFile


class Workout:
    def __init__(self, pwx_path: str, athlete: Athlete = Athlete()):
        pwx_file = PwxFile(pwx_path)
        self.athlete = athlete
        self.athlete_name: Optional[str] = pwx_file.athlete_name
        self.title: Optional[str] = pwx_file.title
        self.start_time: Optional[datetime.datetime] = pwx_file.start_time
        self.sport: Optional[str] = pwx_file.sport
        self.segments = pd.DataFrame(pwx_file.segments)
        self.samples = pd.DataFrame(
            pwx_file.samples,
            columns=("time", "heartrate", "speed", "power", "cadence", "distance"),
        )
        self.time_features()
        self.athlete_features()

    def time_features(self):
        self.samples["minute"] = self.samples.time / 60

    def athlete_features(self):
        if self.athlete.ftp is not None:
            self.samples["ftp_power"] = self.samples.power / self.athlete.ftp
            self.samples["power_zone"] = self.samples.power.apply(
                self.athlete.get_power_zone
            )
        if self.athlete.map is not None:
            self.samples["map_power"] = self.samples.power / self.athlete.map
        if self.athlete.ac is not None:
            self.samples["ac_power"] = self.samples.power / self.athlete.ac
        if self.athlete.nm is not None:
            self.samples["nm_power"] = self.samples.power / self.athlete.nm
        if self.athlete.lthr is not None:
            self.samples["lthr_hr"] = self.samples.heartrate / self.athlete.lthr
            self.samples["hr_zone"] = self.samples.heartrate.apply(
                self.athlete.get_hr_zone
            )

    def power_chart(self, width: int = 900, height: int = 300) -> alt.Chart:
        alt.themes.enable("none")

        pwr_zone_scheme = [
            "#76d478",
            "#3ea048",
            "#186c20",
            "#97c4fc",
            "#295eaa",
            "#ffc500",
            "#ff8400",
            "#ff0000",
        ]

        hr_zone_scheme = [
            "#76d478",
            "#186c20",
            "#97c4fc",
            "#ffc500",
            "#ff8400",
            "#ff0000",
        ]

        x_encoding = alt.X("minute:Q")

        power: alt.Chart = (
            alt.Chart(self.samples)
            .mark_bar(tooltip=True, width=0.5)
            .encode(
                x=x_encoding,
                y=alt.Y("power:Q", axis=alt.Axis(title="Power (W)")),
                color=alt.Color(
                    "power_zone:N", scale=alt.Scale(range=pwr_zone_scheme),
                ),
            )
            .properties(width=width, height=height, title=self.title)
        )

        max_power = self.samples.power.max()
        metrics = self.athlete.four_dp()

        four_dp_rules = (
            alt.Chart(metrics[((metrics.power * 0.80) < max_power)])
            .mark_rule()
            .encode(y=alt.Y("power:Q", axis=alt.Axis(title="Power (W)")),)
        )

        hr = (
            alt.Chart(self.samples)
            .mark_line(color="grey", tooltip=True)
            .encode(
                x="minute:Q", y=alt.Y("heartrate:Q", axis=alt.Axis(title="Heart Rate"))
            )
        )

        hr_heat = (
            alt.Chart(self.samples)
            .mark_tick(tooltip=True)
            .encode(
                x=x_encoding,
                color=alt.Color("hr_zone:N", scale=alt.Scale(range=hr_zone_scheme)),
            )
            .properties(width=width, height=25, title="Heart Rate Zone")
        )

        power_charts = alt.layer(power, four_dp_rules)

        layered_chart = alt.layer(power_charts, hr).resolve_scale(y="independent")

        return alt.vconcat(power_charts, hr_heat).configure_scale(continuousPadding=0)
