import os
SECRET_KEY = os.urandom(32)

basedir = os.path.abspath(os.path.dirname(__file__))
DEBUG = True

SQLALCHEMY_DATABASE_URI = 'sqlite:///'+os.path.join(basedir, 'res'+os.sep+'exchange_rate.db')
SQLALCHEMY_TRACK_MODIFICATIONS = False
