# Managers Import Spec (ManagerReport)
## Input File
- Expected source: ManagerReport-YYYY_MM_DD-XXXX.xlsx

## Field Mapping
- manager_id ← "کد مدیر" (اگر نبود، هش از نام+شماره)
- name ← "نام مدیر"
- relations:
  - mentors.manager_id باید به این جدول وصل شود.

## Source Columns (auto-detected snapshot)
{
  "path": "/mnt/data/ManagerReport-1404_05_19-3570.xlsx",
  "sheets": [
    "ManagerList3570"
  ],
  "columns": [
    "کد نمایندگی",
    "کد کارمندی مدیر",
    "کد مدیر",
    "نام مدیر",
    "جنسیت",
    "موبایل",
    "قبلا پشتیبان بود",
    "عادی",
    "اموزشگاه",
    "مدرسه",
    "شمارنده",
    "تعداد پشتیبان عادی",
    "تعداد پشتیبان آموزشگاه",
    "تعداد پشتیبان مدرسه"
  ]
}
