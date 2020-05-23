import datetime
import xml.etree.ElementTree as ET
from typing import List, NamedTuple, Optional


class SummaryData(NamedTuple):
    beginning: Optional[float]
    duration: Optional[float]
    durationstopped: Optional[float]
    dist: Optional[float]


class Segment(NamedTuple):
    name: Optional[str]
    beginning: Optional[float]
    duration: Optional[float]
    durationstopped: Optional[float]
    dist: Optional[float]


class Sample(NamedTuple):
    timeoffset: Optional[float]
    hr: Optional[float]
    spd: Optional[float]
    pwr: Optional[float]
    cad: Optional[float]
    dist: Optional[float]


class PwxFile:
    def __init__(self, pwx_path: str):

        self.path: str = pwx_path
        self.root: ET.Element = ET.parse(pwx_path).getroot()

        assert self.root is not None
        self.ns: str = self.get_ns()
        self.workout: ET.Element = self.get_workout()
        self.athlete_name: Optional[str] = self.get_athlete_name()
        self.sport: Optional[str] = self.get_sport()
        self.title: Optional[str] = self.get_title()
        self.start_time: Optional[datetime.datetime] = self.get_start_time()
        self.summary_data: Optional[SummaryData] = self.get_summary_data(self.workout)
        self.segments: List[Segment] = self.get_segments()
        self.samples: List[Sample] = self.get_samples()

    def get_ns(self) -> str:
        for key, val in self.root.items():
            if "schemaLocation" in key:
                ns = f"{{{val.split()[0]}}}"
        assert ns is not None
        return ns

    def find(self, root: ET.Element, tag: str) -> Optional[ET.Element]:
        return root.find(f"{self.ns}{tag}")

    def find_datum(self, root: ET.Element, tag: str) -> Optional[float]:

        elem = self.find(root, tag)
        if elem is not None:
            elem_datum = elem.text
        else:
            elem_datum = None

        if elem_datum is not None:
            datum: Optional[float] = float(elem_datum)
        else:
            datum = None
        return datum

    def get_workout(self) -> ET.Element:
        workout = self.find(self.root, "workout")
        assert workout is not None
        return workout

    def get_workout_elem(self, tag: str) -> Optional[ET.Element]:
        assert self.workout is not None
        return self.find(self.workout, tag)

    def get_athlete_name(self) -> Optional[str]:
        athlete_elm = self.get_workout_elem("athlete")

        assert athlete_elm is not None
        athlete_name = self.find(athlete_elm, "name")

        assert athlete_name is not None
        return athlete_name.text

    def get_sport(self) -> Optional[str]:
        sport_type_elem = self.get_workout_elem("sportType")

        assert sport_type_elem is not None
        return sport_type_elem.text

    def get_title(self) -> Optional[str]:
        title_elm = self.get_workout_elem("title")

        assert title_elm is not None
        return title_elm.text

    def get_start_time(self) -> Optional[datetime.datetime]:
        time_elm = self.get_workout_elem("time")

        assert time_elm is not None
        time_string = time_elm.text

        assert time_string is not None
        return datetime.datetime.fromisoformat(time_string)

    def get_summary_data(self, root: ET.Element) -> SummaryData:
        summary_elem = self.find(root, "summarydata")
        assert summary_elem is not None
        fields = SummaryData._fields
        data = {}
        for field in fields:
            data[field] = self.find_datum(summary_elem, field)
        return SummaryData(**data)

    def get_segments(self) -> List[Segment]:
        segments = []
        for elem in self.workout:
            if "segment" in elem.tag:
                name: Optional[str] = None
                name_elm = self.find(elem, "name")
                if name_elm is not None:
                    name = name_elm.text

                segment = Segment(name, *self.get_summary_data(elem))
                segments.append(segment)
        return segments

    def get_sample_data(self, sample: ET.Element) -> Sample:
        fields = Sample._fields
        data = {}
        for field in fields:
            data[field] = self.find_datum(sample, field)
        return Sample(**data)

    def get_samples(self) -> List[Sample]:
        samples = []
        for elem in self.workout:
            if "sample" in elem.tag:
                sample = self.get_sample_data(elem)
                samples.append(sample)
        return samples
