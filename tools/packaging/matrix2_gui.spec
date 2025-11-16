# -*- mode: python ; coding: utf-8 -*-
"""Spec PyInstaller برای ساخت نسخهٔ GUI برنامه Matrix2."""

import pathlib

project_root = pathlib.Path(__file__).resolve().parents[2]
dist_datas = []
config_dir = project_root / "config"
if config_dir.exists():
    dist_datas.append((str(config_dir), "config"))

assets_dir = project_root / "app" / "ui" / "assets"
if assets_dir.exists():
    dist_datas.append((str(assets_dir), "app/ui/assets"))

block_cipher = None

a = Analysis(
    ['run_gui.py'],
    pathex=[str(project_root)],
    binaries=[],
    datas=dist_datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Matrix2-GUI',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=True,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Matrix2-GUI',
)
