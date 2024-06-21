import sys   
from wrds import Connection, __version__ as wrds_version
from sys import version_info

appname = "{0} python {1}.{2}.{3}/wrds {4}".format(
    sys.platform, version_info[0], version_info[1], version_info[2], wrds_version
)

# Sane defaults
WRDS_POSTGRES_HOST = ""
WRDS_POSTGRES_PORT = 9737
WRDS_POSTGRES_DB = "wrds"
WRDS_CONNECT_ARGS = {"sslmode": "require", "application_name": appname}

class NotSubscribedError(PermissionError):
    pass


class SchemaNotFoundError(FileNotFoundError):
    pass


class Connection_remote(Connection):

    def __init__(self, autoconnect=True, verbose=False, **kwargs):
        super().__init__(autoconnect, verbose, **kwargs)
        # Adding functionality of accepting wrds_password
        self._password = kwargs.get("wrds_password", "")

    def __make_sa_engine_conn(self, raise_err=False):
         super()._Connection__make_sa_engine_conn()
            
    def __create_pgpass_file_win32(self):
        super()._Connection__create_pgpass_file_win32()
    
    def __create_pgpass_file_unix(self):
        super()._Connection____create_pgpass_file_unix()

    def __write_pgpass_file(self, pgfile):
        super()._Connection_write_pgpass_file(pgfile)   

    def __check_schema_perms(self, schema):
        super()._Connection__check_schema_perms(schema)
    
    def __get_user_credentials(self):
        """
        Overwriting to avoid input.

        Use the OS-level username as a default so the user
        doesn't have to reenter it if they match.
        Return both the username and the password.
        """
        # get username
        if self._username:
            username = self._username
        else:
            username = ''
            RuntimeWarning('No username found in Connection object.')
        # get password
        if self._password:
            passwd = self._password
        else:
            passwd = ''
            RuntimeWarning('No username found in Connection object.')
        
        return username, passwd
    

    def connect(self):
        """
        Overwriting to avoid input,
        
        Make a connection to the WRDS database."""
        # first try connection using system defaults and params set in constructor
        self.__make_sa_engine_conn()

        if (self.engine is None and self._hostname != WRDS_POSTGRES_HOST):
            # try explicit w/ default hostname
            print(f"Trying '{WRDS_POSTGRES_HOST}'...")
            self._hostname = WRDS_POSTGRES_HOST
            self.__make_sa_engine_conn()
        
        if (self.engine is None):
            # Use explicit username and password
            self._username, self._password = self.__get_user_credentials()
            # Last attempt, raise error if Exception encountered
            self.__make_sa_engine_conn(raise_err=True)

            if (self.engine is None):
                print(f"Failed to connect {self._username}@{self._hostname}")
            else:
                # Connection successful. Offer to create a .pgpass for the user.
                print("WRDS recommends setting up a .pgpass file. Defaulting to yes.")
                do_create_pgpass = "y"

                # While loop should never be entered
                while do_create_pgpass != "y" and do_create_pgpass != "n":
                    do_create_pgpass = input("Create .pgpass file now [y/n]?: ")

                if do_create_pgpass == "y":
                    try:
                        self.create_pgpass_file()
                        print("Created .pgpass file successfully.")
                    except Exception:
                        print("Failed to create .pgpass file.")
                print(
                    "You can create this file yourself at any time "
                    "with the create_pgpass_file() function."
                )
