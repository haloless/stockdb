
call %USERPROFILE%\Anaconda3\Scripts\activate.bat

set FLASK_APP=dbapp.py

set FLASK_DEBUG=1

flask run

