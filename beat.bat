@echo off
celery --workdir=/full/path/to/middleware-folder -A bein_sports beat
if NOT ["%errorlevel%"]==["0"] (
    pause
    exit /b %errorlevel%
)