Install python 3.6.4 on linux mint: https://tecadmin.net/install-python-3-6-ubuntu-linuxmint/
More information on install python on linux mint: https://mintguide.org/other/794-python-3-6-install-latest-version-into-linux-mint.html

Installion ibapi on linux
=========================

(1) go to https://interactivebrokers.github.io/
(2) download the software for your OS
(3) follow instructions on https://ibkr.info/article/2484
(4) install the package: sudo python3.6 setup.py install
(5) cp /usr/local/lib/python3.6/site-packages/ibapi-9.73.7-py3.6.egg /home/marco/github/TFS/tfs_env/lib/python3.6/site-packages
(6) cp -Rp ~/IBJts/source/pythonclient/ibapi [working_directory=TFS/tfs]
(7) create file logs/log.log in working directory (TFS/tfs)

MAC OS instructions to use the seaborn/matplotlib module
========================================================
https://stackoverflow.com/questions/21784641/installation-issue-with-matplotlib-python

Create requirements file
========================
pip freeze > requirements.txt
