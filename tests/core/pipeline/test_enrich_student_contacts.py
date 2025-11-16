from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

_HERE = Path(__file__).resolve()
sys.path.insert(0, str(_HERE.parents[3]))

from app.core.pipeline import CONTACT_POLICY_ATTR, enrich_student_contacts


def test_enrich_student_contacts_sets_attr_flag() -> None:
    df = pd.DataFrame(
        [
            {
                "student_mobile": "9357174851",
                "contact1_mobile": "",
                "contact2_mobile": "",
                "student_landline": "3512345678",
                "student_registration_status": "0",
                "hekmat_tracking": "",
            }
        ]
    )

    enriched = enrich_student_contacts(df)

    assert enriched.attrs.get(CONTACT_POLICY_ATTR) is True
