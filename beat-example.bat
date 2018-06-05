@echo off
celery --workdir=C:\Users\HP\Documents\bein_sports -A bein_sports beat
if NOT ["%errorlevel%"]==["0"] (
    pause
    exit /b %errorlevel%
)