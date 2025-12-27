import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
QUALITY_REPORT = os.path.join(
    BASE_DIR,
    "data",
    "processed",
    "quality_report.json"
)

def test_quality_report_exists():
    assert os.path.exists(QUALITY_REPORT)

def test_quality_score_range():
    with open(QUALITY_REPORT) as f:
        report = json.load(f)

    score = report.get("quality_score") or report.get("summary", {}).get("quality_score")
    assert score is not None, "quality_score not found in report"
    assert 0 <= score <= 100
