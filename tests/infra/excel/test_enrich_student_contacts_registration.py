from __future__ import annotations

import pandas as pd

from app.core.pipeline import enrich_student_contacts


def test_enrich_student_contacts_prefers_explicit_status_over_finance() -> None:
    students = pd.DataFrame(
        {
            "student_id": ["S1", "S2", "S3"],
            "وضعیت ثبت نام": [0, 1, 3],
            "مالی حکمت بنیاد": [3, 0, 0],
        }
    )

    enriched = enrich_student_contacts(students)

    assert enriched["student_registration_status"].tolist() == [0, 1, 3]
    assert enriched["وضعیت ثبت نام"].tolist() == [0, 1, 3]


def test_enrich_student_contacts_falls_back_to_finance_when_missing_status() -> None:
    students = pd.DataFrame(
        {
            "student_id": ["S1", "S2"],
            "student_finance": [0, 3],
        }
    )

    enriched = enrich_student_contacts(students)

    assert enriched["student_registration_status"].tolist() == [0, 3]
