import logging
import os
import json
import pandas as pd
from elasticsearch import Elasticsearch
from datetime import datetime


class MyLogger():
    """This class initialize personal logger"""

    def __init__(self):
        # creating logger with script name
        self.logger = logging.getLogger("ElasticManager")
        self.logger.setLevel(logging.DEBUG)
        # creating file handler where the logs storage
        fh = logging.FileHandler(__name__ + ".log")
        fh.setLevel(logging.DEBUG)
        # creating console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # creating formatters
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # finally we add handlers to our logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)


class Error(Exception):
    """Base class for excpetion"""
    pass


class StatusCodeError(Error):
    """Exception raised for errors
            when status code is not 200"""

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class ElasticManager(MyLogger):

    def __init__(self):

        super(ElasticManager, self).__init__()
        self.logger.debug
        self.ES = Elasticsearch()
        self.indices = list(self.ES.indices.get_alias())

        self.choose_option()

    def choose_option(self):
        print("--- Elastic manager ---\n Please select option:")
        option_list = ["LIST ALL INDECIES",
                       "CREATE_BACKUP",
                       "DELETE INDEX",
                       "DELETE TIME INTERVAL",
                       "UPLOAD DATA (UPLOAD ONLY LATENCY)",
                       "RESTORE DATA",
                       "CREATE INDEX",
                       "DELETE REPOSITORY"]
        for number, option in enumerate(option_list):
            print("{0}.{1}".format(number + 1, option))

        choice = int(input("Your input: "))

        if (choice == 1):
            for number, item in enumerate(self.indices):
                print('{}. {}'.format(number, item))
        elif (choice == 2):
            self.select_index()
            self.create_backup()
        elif (choice == 3):
            self.select_index()
            self.delete_index()
        elif(choice == 4):
            self.select_index()
            self.query_delete()
        elif(choice == 5):
            self.select_index()
            self.upload_data()
        elif(choice == 6):
            #self.select_index()
            self.restore_data()
        elif(choice == 7):
            self.create_index()
        elif(choice == 8):
            self.delete_repository()
        else:
            print("Select correct number")

    def select_index(self):
        # logic_list = ["tests", "iterations", '.']
        print("Please select desired index:")

        for number, item in enumerate(self.indices):
            print('{}. {}'.format(number, item))

        self.selected_index = int(input("\nYour input:"))

        try:
            self.index_name = self.indices[self.selected_index]
            print(" ---{} selected--- ".format(self.index_name))
        except IndexError:
            print("Number must be in the range: 0-{}".format(len(self.indices)))

    def create_backup(self):
        self.logger.info("Creating logical repository")
        self.logger.debug("Opening snapshot body json")

        with open("./es_snap_creating_repo.json") as file:
            body = json.load(file)
        # Modifing json filie to specify snapshot directory name
        self.logger.debug("Change file location content")
        body["settings"]["location"] = self.indices[self.selected_index] + \
            "_" + "_".join(str(datetime.now()).split(' '))

        self.logger.debug(type(body))
        self.logger.debug(json.dumps(body, indent=4))
        self.logger.debug("File location modified")

        answer = input("Do you want to create new repository?(y/n): ")
        # Option for creating new repository
        if answer == "y":
            self.repository = input("Name your respository: ")
            self.ES.snapshot.create_repository(
                repository=self.repository, body=body)
            self.logger.info("Repository name".format())

        else:

            self.choose_repository()
            self.ES.snapshot.create_repository(
                repository=self.repository, body=body)
            self.logger.info("Repository name".format(self.repository))

        # Creating snapshot
        with open("snapshot_mainbody.json", "r") as file:
            snapshot_mainbody = json.load(file)
            snapshot_mainbody["indices"] = self.indices[self.selected_index]

            self.logger.debug(json.dumps(snapshot_mainbody, indent=4))

            self.logger.debug("%s", self.indices[self.selected_index])

            self.ES.snapshot.create(repository=self.repository,
                                    snapshot=body["settings"]["location"],
                                    wait_for_completion=True,
                                    body=snapshot_mainbody,
                                    master_timeout="60s")

        self.logger.info("Process completed")

    def upload_data(self):
        data = pd.read_csv("data.csv", delimiter=';', header=0, index_col=0)
        columns_to_leave = ["latency", "device_id",
                            "source", "value", "device_name", "unix_time"]
        data = data.loc[:][columns_to_leave]
        data = data[data.source != "BOT"]
        self.logger.debug(self.index_name)
        counter = 0
        temp = json.loads(data.to_json(orient="records"))
        for element in temp:
            counter += 1
            self.ES.index(index=self.index_name,
                          body=json.dumps(element), doc_type="event")
            if counter % 100 == 0:
                self.logger.info("%d logs uploaded", counter)
        self.logger.info("Upload completed")

    def delete_index(self):
        answer = input(
            "Are you sure to delete {}?(y/n) ".format(self.index_name))
        if answer == "y":
            self.ES.indices.delete(self.index_name)
            self.logger.info("Index deleted")
        else:
            print("Deleting aborted")

    def create_index(self):
        with open("mapping.json", 'r') as mapping:
            name = input("Please insert index name: ")
            self.ES.indices.create(name, body=mapping.read())
            self.logger.info("---{}--- created".format(name))

    def query_delete(self):
        with open("query_delety.json", "r") as file:
            delete_body = json.load(file)
            delete_body["query"]["range"]["unix_time"]["gte"] = self.format_date(
                "start")
            delete_body["query"]["range"]["unix_time"]["lt"] = self.format_date(
                "end")

            self.logger.debug(json.dumps(delete_body, indent=4))
            self.ES.delete_by_query(index=self.index_name, body=delete_body)
            self.logger.info("Results deleted")

    def format_date(self, name):
        print("Please input {} date: (eg.2000/01/01/15/00)".format(name))
        year, month, day, hour, minute = input().split("/")
        time = "{}-{}-{}T{}:{}:00.000Z".format(
            year, month, day, hour, minute)
        self.logger.debug(time)
        return time

    def restore_data(self):
        with open("restore_data.json", 'r') as restore_json:
            read_json = json.load(restore_json)

            self.choose_repository()
            snapshot = self.ES.snapshot.get_repository(
            )[self.repository]["settings"]["location"]

            self.logger.debug(snapshot)
            self.logger.debug("Withdrawing name of index from snapshot name")
            self.logger.info("_".join(snapshot.split("_")[:-2]))
            self.ES.snapshot.restore(repository=self.repository, snapshot=snapshot, wait_for_completion=True)

    def choose_repository(self):
        repository_list = list(self.ES.snapshot.get_repository().keys())
        print("Repositories available:")
        for number, repository in enumerate(repository_list):
            print("{}.{}".format(number + 1, repository))
        repository = int(input("Choose repository by giving its number: "))
        repository -= 1
        self.repository = repository_list[repository]
        print("Repository ----{}---- selected".format(self.repository))

    def delete_repository(self):
        self.choose_repository()
        self.ES.snapshot.delete_repository(repository=self.repository)
        self.logger.info("{} deleted".format(self.repository))


ElasticManager()
