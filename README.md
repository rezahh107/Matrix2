# سامانه تخصیص دانشجو-منتور (نسخه معماری مدولار)

## اجرا
```bash
pip install -r requirements.txt
python -m app.main
```

## ساخت فایل اجرایی (PyInstaller)
```bash
pyinstaller --onefile --windowed --name تخصیص_دانشجو_منتور \
  --collect-all PySide6 --hidden-import openpyxl --hidden-import pandas.io.formats.excel \
  app/main.py
```
