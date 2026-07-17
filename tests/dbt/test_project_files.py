from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_dbt_project_has_tempo_no2_layers():
    project = (ROOT / "dbt" / "dbt_project.yml").read_text()
    assert "tempo_no2_staging" in project
    assert "tempo_no2_marts" in project
    assert "tempo_no2_observability" in project
