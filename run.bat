REM @echo off

REM please set this to your pythin path!!!
REM start %USERPROFILE%\Anaconda3\python dbapp.py

call %USERPROFILE%\Anaconda3\Scripts\activate.bat
python dbapp.py

REM cmd /K "call %USERPROFILE%\Anaconda3\Scripts\activate.bat & python dbapp.py"

REM %USERPROFILE%\Anaconda3\python dbapp.py


pause
