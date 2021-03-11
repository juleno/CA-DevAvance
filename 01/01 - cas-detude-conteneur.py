#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Server side entrypoint, as a Flask application."""
import json
from flask import Flask, request, jsonify, g
import settings # external Python file describing application settings

MYAPP = Flask(__name__, static_folder=settings.yangPath + '/amadeus2/')


@MYAPP.route('/')
@MYAPP.route('/ui/<localparams>')
@MYAPP.route('/static/<path:file>')
def root(file='index.html', localparams=''):
    """Supply any browser like assets (scripts, css, images, etc..)."""
    return MYAPP.send_static_file(file)


@MYAPP.route('/<path:file>', methods=["GET"])
def static_files_fallback(file: str):
    return MYAPP.send_static_file(file)


@MYAPP.route('/login', methods=["POST"])
def login() -> json:
    """Erase database stored session and confirm."""
    # conn = db.Client(login)
    # mongo = db.Client.GlobalDirectory()
    # desc = mongo.get_business_database_descriptor(login)
    # database.Client()._instantiate_business_db(login, password)

    # Checking login/pass
    login = req.json["login"]
    password = req.json["password"]

    client = database.Driver()
    queryFilter = {
        "$and": [
            {"users.identifiers.type": "nucleotic"},
            {"users.identifiers.login": login},
            {"identifiers.password": hashlib.sha512((password + hashSalt).encode('utf-8')).hexdigest()},CNI + retaper temoignage (cf. terams Claire)
        ]
    }
    resultSet = client.global_driver.find('Licenses', queryFilter)
    if not resultSet:
        return False

    # Isolating the right user within the licence
    user = [user for user in resultSet[0]['users']
            for identifier in user["identifiers"] if identifier['type'] == "nucleotic" and identifier['login'] == login][0]

    # Building session
    sessionData = _create_session(user)
    session['token'] = sessionData["token"]
    user.pop('identifiers')
    session['user'] = user

    # Initializing BUSINESS DATABASE connection
    client.connect(resultSet[0]["databases"][0])

    session['user'] = user
    pprint(session.get('user'))
    return jsonify(session.get('user'))


@MYAPP.route('/logout', methods=["POST"])
def logout() -> json:
    """Erase database stored session and confirm."""
    # TODO: revoke/recreate user key 
    return jsonify(NotificationsCodes.session_destroyed)


if __name__ == "__main__":
    MYAPP.run(debug=True,
              host=settings.httpServer['host'],
              port=settings.httpServer['port']
              )
