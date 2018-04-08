class database_adapter(object):
    def __init__(this, settings):
        this.host = settings['host']
        this.port = settings['port']
        this.database = settings['database']
        this.username = settings['username']
        this.password = settings['password']

    @property
    def host(this):
        return this._host

    @host.setter
    def host(this, value):
        this._host = value

    @property
    def port(this):
        return this._port

    @port.setter
    def port(this, value):
        this._port = value

    @property
    def database(this):
        return this._database

    @database.setter
    def database(this, value):
        this._database = value

    @property
    def username(this):
        return this._username

    @username.setter
    def username(this, value):
        this._username = value

    @property
    def password(this):
        return this._password

    @password.setter
    def password(this, value):
        this._password = value
