from __future__ import (absolute_import, division, print_function)

import configparser

import wof
import wof.flask
from wof.examples.flask.odm2.timeseries.odm2_timeseries_dao import Odm2Dao
#import private_config

"""
    python runserver_odm2_timeseries.py
    --config=odm2_config_timeseries.cfg

"""
#logging.basicConfig(level=logging.DEBUG)
#logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)


def startServer(conf='odm2_config_timeseries.cfg',
                    openPort = 8080):

    config = configparser.ConfigParser()
    with open(conf, 'r') as configfile:
        config.read_file(configfile)
        connection = (config['DATABASE']['Engine'],
                      config['DATABASE']['Address'],
                      config['DATABASE']['Db'],
                      config['DATABASE']['User'],
                      config['DATABASE']['Password'])

    dao = Odm2Dao(*connection)

    varcode = dao.get_all_variables()[0].VariableCode
    sitecode = dao.get_all_sites()[0].SiteCode
    print(varcode, sitecode)
    print("DataType :: ", dao.get_variable_by_code(varcode).DataType)
    from datetime import datetime
    print(dao.get_datavalues(site_code=sitecode, var_code=varcode, begin_date_time=datetime(2007, 8, 16), end_date_time=datetime(2007, 8, 17)))

    app = wof.flask.create_wof_flask_app(dao, conf)
    app.config['DEBUG'] = True


    url = "http://127.0.0.1:" + str(openPort)
    print("----------------------------------------------------------------")
    print("Access Service endpoints at ")
    for path in wof.site_map(app):
        print("{}{}".format(url, path))

    print("----------------------------------------------------------------")

    app.run(host='0.0.0.0', port=openPort, threaded=True)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='start WOF for an ODM2 database.')
    parser.add_argument('--config',
                       help='Configuration file', default='odm2_config_timeseries.cfg')

    parser.add_argument('--port',
                       help='Open port for server."', default=8080, type=int)
    args = parser.parse_args()
    print(args)

    startServer(conf=args.config, openPort=args.port)
