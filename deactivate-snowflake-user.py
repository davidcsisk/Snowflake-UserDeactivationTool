import os
import sys
import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
from datetime import datetime

if len(sys.argv) == 1:
   print("")
   print("Usage:  python deactivate-snowflake-user.py login-name [--deactivate]")
   print("        -  login-name arg is mandatory and will be the email address of the user")
   print("           If the full email address is not known, run the command with the known AD username to get the full login name, then re-run with login name if necessary")
   print("")
   print("        -  if '-deactivate' arg is not supplied, an interactive prompt will be presented")
   print("")
   print("Note: Requires snowflake python connector.  If not already installed:")
   print("           pip install snowflake-connector-python==2.7.0")
   print("      Following environment variables must be configured, including a valid admin user for the service account:")
   print("           SDLadminUser,SDLadminPass,SDLaccount,SDLwarehouse,SDLrole,SDLdatabase,SDLschema")
   print("")
   quit()

search_user = sys.argv[1].upper()
deactivate = ''

if len(sys.argv) == 3:
    if sys.argv[2].lower() == '--deactivate':
        deactivate = 'Y'

conn = snowflake.connector.connect(
    converter_class=SnowflakeNoConverterToPython,
#    authenticator='externalbrowser',
    user=os.getenv('SDLadminUser'),
    password=os.getenv('SDLadminPass'),
    account=os.getenv('SDLaccount'),
    warehouse=os.getenv('SDLwarehouse'),
    role=os.getenv('SDLrole'),
    database=os.getenv('SDLdatabase'),
    schema=os.getenv('SDLschema')
    )

curs = conn.cursor()

try:
    proceed = ''
    user_name = ''
    login_name = ''
    is_disabled = ''
    state = ''
    row_count = 0

    # This query returns 1 or 0 rows
    curs.execute("select name, login_name, disabled from snowflake.account_usage.users where login_name like '" + search_user + "%' and deleted_on is null;")
    result = curs.fetchall()
    for row in result:
        row_count = row_count + 1
        user_name = row[0]
        login_name = row[1]
        is_disabled = row[2]
        if is_disabled == 'true':
            state = 'DEACTIVATED'
        elif is_disabled == 'false':
            state = 'ACTIVE'
        print("search=" + search_user + " user_name=" + user_name + " / login_name=" + login_name + " state=" + state)
            
    if row_count == 0:
        print(datetime.now(), ": search=" + search_user + " - No matching user found")
    elif row_count > 1:
        print("WARNING: More than 1 matching user found...examine login_name list and re-run with correct search citeria.")
        quit()       
      
    if deactivate == 'Y' and state == 'ACTIVE':
        curs.execute("use role ACCOUNTADMIN;")
        curs.execute("alter user " + user_name + " set disabled = true;")
        print(datetime.now(), ": user=" + user_name + " / login_name=" + login_name + " deactivated")
    elif deactivate == '' and state == 'ACTIVE':
        proceed = input("   -> Deactivate user=" + user_name + " / login_name=" + login_name + " now? Enter 'Y' to proceed, leave blank to abort: ")

    if proceed == 'Y' and state == 'ACTIVE':
        curs.execute("use role ACCOUNTADMIN;")
        curs.execute("alter user " + user_name + " set disabled = true;")
        print(datetime.now(), ": user=" + user_name + " / login_name=" + login_name + " deactivated")

finally:
    curs.close()
conn.close()

