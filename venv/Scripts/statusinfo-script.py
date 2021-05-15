#!"C:\Users\Praveen Karthik\PycharmProjects\RealTimeMonitoringDashBoard\venv\Scripts\python.exe"
# EASY-INSTALL-ENTRY-SCRIPT: 'statusinfo==0.1.6','console_scripts','statusinfo'
__requires__ = 'statusinfo==0.1.6'
import re
import sys
from pkg_resources import load_entry_point

if __name__ == '__main__':
    sys.argv[0] = re.sub(r'(-script\.pyw?|\.exe)?$', '', sys.argv[0])
    sys.exit(
        load_entry_point('statusinfo==0.1.6', 'console_scripts', 'statusinfo')()
    )
