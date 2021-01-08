Install python 3.6.4 on linux mint: https://tecadmin.net/install-python-3-6-ubuntu-linuxmint/
More information on install python on linux mint: https://mintguide.org/other/794-python-3-6-install-latest-version-into-linux-mint.html

Installation ibapi on linux mint
==============================

(1) go to https://interactivebrokers.github.io/
(2) download the software for your OS
(3) follow instructions on https://ibkr.info/article/2484
(4) install the package: sudo python3.6 setup.py install
(5) cp /usr/local/lib/python3.6/site-packages/ibapi-9.73.7-py3.6.egg [working_dir=TFS/tfs_env/lib/python3.6/site-packages]
(6) cp -Rp ~/IBJts/source/pythonclient/ibapi [working_directory=TFS/tfs]
(7) create file logs/log.log in working directory (TFS/tfs)

MAC OS instructions to use the seaborn/matplotlib module
========================================================
https://stackoverflow.com/questions/21784641/installation-issue-with-matplotlib-python

Create requirements file
========================
pip freeze > requirements.txt

Install SQLite on windows
=========================

Step 1 − Go to SQLite download page, and download precompiled binaries from Windows section.

Step 2 − Download sqlite-dll-win32-x86-3310100.zip, sqlite-dll-win64-x64-3310100.zip and sqlite-tools-win32-x86-3310100.zip

Step 3 − Create a folder C:\>sqlite and unzip above two zipped files in this folder, which will give you sqlite3.def, sqlite3.dll and sqlite3.exe files.

Step 4 − Add C:\>sqlite in your PATH environment variable and finally go to the command prompt and issue sqlite3 command, which should display the following result.

Installing TA-Lib on windows
============================
- check your python version (e.g. 3.6)
- go to https://www.lfd.uci.edu/~gohlke/pythonlibs/#ta-lib and download the whl file with the correct python number, e.g. for python 3.6 look for file name with cp36 in the name.
- pip install <name of the whl file>, e.g. 'pip install TA_Lib-0.4.17-cp36-cp36m-win_amd64.whl'

For overview of the available indicators go to http://tadoc.org/. For python examples go to https://mrjbq7.github.io/ta-lib/doc_index.html
