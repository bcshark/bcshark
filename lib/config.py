import json

class JsonConfigurationManager(object):
    def __init__(this, config_file):
        with open(config_file, 'r') as _config_file:
            this._config = json.load(_config_file)

    @property
    def settings(this):
        return this._config;

class ConfigurationManager(object):
    def __init__(this, config_file):
        this.config_manager = JsonConfigurationManager(config_file)

    @property
    def settings(this):
        return this.config_manager.settings;

    def __setitem__(this, key, value):
        this.config_manager.settings[key] = value

    def __getitem__(this, key):
        return this.config_manager.settings[key]

    def has_key(this, key):
        return this.config_manager.settings.has_key(key)
