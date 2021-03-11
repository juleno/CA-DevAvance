#!/usr/bin/env python
# -*- coding: utf-8 -*-

from flask import g
from pymongo import MongoClient
from datetime import datetime
from bson.codec_options import CodecOptions
from bson import ObjectId
from core.lib import entity_tools
import pytz
import logging
import settings


class Driver:
    """Initialize Global directory + Business database.

    :var self.instance:
         is based on the g.BUSINESS_DATABASE object. That allows a single
         connection for the whole query treatment
    :var self.glabal_driver:
         provides an access to the nested global directory class
    """

    instance = None
    global_driver = None
    tz_awareness = True

    def __init__(self):
        """Set Global database socket + Business DB socket fitted to the users credentials."""
        self.global_driver = self._Global()
        if "BUSINESS_DATABASE" in g:
            self.instance = g.BUSINESS_DATABASE

    def connect(self, business_database_descriptor: dict) -> None:
        """Open connection to the Business database."""
        if "BUSINESS_DATABASE" not in g:
            logging.info("Opening Business Database connection")
            print(self.global_driver.get_connection_string(business_database_descriptor))
            g.BUSINESS_DATABASE = MongoClient(
                self.global_driver.get_connection_string(business_database_descriptor),
                serverSelectionTimeoutMS=3000
            )[business_database_descriptor['dbName']]
        self.instance = g.BUSINESS_DATABASE

    def find(self, collectionName: str, queryFilter: dict = None, projector: dict = None, skip: int = 0, limit: int = 0,
             sort: list = None) -> list:
        """Return the DB resultset, as dict instead of Cursor, compatible with Flask."""
        # pymongo API requires a non empty sort list, considering _id as the best default option ATM.
        response = self.instance[collectionName].with_options(
                    codec_options=CodecOptions(tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))) \
                            .find(queryFilter, projector) \
                            .skip(skip) \
                            .limit(limit)
        if sort:
            response = response.sort(sort)
        return [item for item in response]

    def count(self, collection_name: str, query_filter: dict = None):
        return self.instance[collection_name].estimated_document_count()

    def save(self, collection_name: str, document: dict):
        """Insert (or update if _id exists) the given document within collectionName."""
        if "_id" in document and document['_id'] is not None:
            document['_id'] = ObjectId(document['_id'])
            return self.replace(collection_name, document, {'_id': document['_id']})
        else:
            if "_id" in document:
                document.pop('_id', None)
            return self.insert(collection_name, document)

    def insert(self, collection_name: str, document: dict) -> dict:
        """Insert a document (without "_id" props) within its wollection."""
        if 'creation' not in document['common']:
            document['common']['creation'] = {
                'date': datetime.now(),
                'author': ObjectId(g.user['_id']),
            }
        document['common']['update'] = {
            'date': datetime.now(),
            'author': ObjectId(g.user['_id']),
        }
        _id = self.instance[collection_name].with_options(codec_options=CodecOptions(
                tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).insert(document)
        logging.info(f"Document inserted succesfully: {_id}")
        return {'_id': _id}

    def replace(self, collection_name: str, new_document: dict, query_filter: dict) -> dict:
        """Replace a document in its collection, plus saving the old version in its "archive" collection."""
        # Grabbing current (ie. database) version of the document
        document = [doc for doc in self.instance[collection_name].with_options(codec_options=CodecOptions(
                tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).find({'_id': new_document['_id']})]

        # Back-it-up as a new version in archive collection
        self.instance[collection_name + "_archived"].with_options(codec_options=CodecOptions(
                tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).insert({
                    'action': 'update',
                    'author': ObjectId(g.user['_id']),
                    'document': document,
                })
        logging.info(f"Document archived within: {collection_name}_archive")

        # Updating meta and inserting the document
        new_document['common']['update'] = {
            'date': datetime.now(),
            'author': ObjectId(g.user['_id']),
        }
        newDocument = self.instance[collection_name].with_options(codec_options=CodecOptions(
                tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).find_one_and_replace(query_filter, new_document)
        logging.info(f"Document updated succesfully: {newDocument['_id']}")

        # Return the whole new document
        return newDocument

    def remove(self, collection_name: str, document_id):
        """Move a document from its collection to its "archived" version."""
        # Ducktyping -> oid & string becomes oid
        document_id = ObjectId(document_id) if isinstance(document_id, str) else document_id

        # Grabbing current (ie. database) version of the document
        document = [item for item in self.instance[collection_name].with_options(codec_options=CodecOptions(
            tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).find({'_id': document_id})]

        # Backing-it-up as a new version in archive collection
        self.instance[collection_name + "_archived"].with_options(codec_options=CodecOptions(
            tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).insert({
                'action': 'remove',
                'author': ObjectId(g.user['_id']),
                'document': document,
            })

        # Remove record from database
        self.instance[collection_name].with_options(codec_options=CodecOptions(
                tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0'))).delete_one({'_id': document_id})
        logging.info(f"Document archived within: {collection_name}_archive")

        # DONE:50 Remove related from Relations documents
        # Cleaning relations involving the deleted document
        return entity_tools.clean_relations_of(ObjectId(document_id))

    class _Global:
        """Driver focused on technical database and the global directory.

        Sub class that isn't supposed to be used that much in the business components.
        Useful only to provide the right business database depending on the provided logons.
        :var self.instance:
             is based on the g.GLOBAL_DIRECTORY object. That allows a single
             connection for the whole query treatment
        """

        instance = None
        tz_awareness = True

        def __init__(self):
            """Build global dir instance if necessary + setting it as self.instance."""
            if "GLOBAL_DIRECTORY" not in g:
                logging.info("Opening GLOBAL DB connection")
                g.GLOBAL_DIRECTORY = MongoClient(
                    self.get_connection_string(),
                    serverSelectionTimeoutMS=3000
                )[settings.database['dbName']]
            self.instance = g.GLOBAL_DIRECTORY

        @staticmethod
        def get_connection_string(descriptor=None) -> str:
            """Build and return connstring from a given (or default) descriptor."""
            descriptor = settings.database if descriptor is None else descriptor
            # To work on an authenticated/unauthenticated database
            if descriptor['login']:
                return 'mongodb://{0}:{1}@{2}:{3}/{4}'.format(
                    descriptor['login'], descriptor['password'],
                    descriptor['host'], descriptor['port'], descriptor['dbName'])
            else:
                return 'mongodb://{0}:{1}/{2}'.format(
                    descriptor['host'], descriptor['port'], descriptor['dbName'])

        def count(self, collection_name: str, query_filter: dict = None):
            return self.instance[collection_name].estimated_document_count()

        def find(self, collection_name: str, query_filter: dict = None, projector: dict = None, skip: int = 0, limit: int = 0) -> list:
            """Run a find query within global database."""
            logging.debug("Query filter for find query on " + collection_name)
            logging.debug(query_filter)
            return [item for item in self.instance[collection_name].with_options(
                codec_options=CodecOptions(tz_aware=self.tz_awareness, tzinfo=pytz.timezone('Etc/GMT+0')))
                .find(query_filter, projector)
                .skip(skip)
                .limit(limit)]

        def save(self, collection_name: str, document: dict):
            """Insert (or update if _id exists) the given document within collectionName."""
            if "_id" in document and document['_id'] is not None:
                document['_id'] = ObjectId(document['_id'])
                return self.replace(collection_name, document, {'_id': document['_id']})
            else:
                if "_id" in document:
                    document.pop('_id', None)
                return self.insert(collection_name, document)

        def insert(self, collection_name, document):
            _id = self.instance[collection_name].with_options(
                codec_options=CodecOptions(
                    tz_aware=self.tz_awareness,
                    tzinfo=pytz.timezone('Etc/GMT+0'))).insert(document)
            logging.info(f"Document inserted succesfully: {_id}")
            return {'_id': _id}

        def replace(self, collection_name, new_document: dict, query_filter: dict) -> dict:
            _id = self.instance[collection_name].with_options(
                codec_options=CodecOptions(
                    tz_aware=self.tz_awareness,
                    tzinfo=pytz.timezone('Etc/GMT+0'))).find_one_and_replace(query_filter, new_document)
            logging.info(f"Document inserted succesfully: {_id}")
            return {'_id': _id}

        def remove(self):
            pass


class NotificationsCodes:
    unavailableGlobalDirectory = {
        "type": "error",
        "code": "0-101",
        "label": "Unreachable global directory"
    }
