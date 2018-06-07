from datetime import datetime
import pandas as pd
import os
import logging


class JoinFile():

    def __init__(self):

        logging.basicConfig()
        self.logger = logging.getLogger('Join Logger')
        self.logger.setLevel(logging.DEBUG)

        self.recognize_order()
        answer = input("Do you want to join these files? y/n")
        if answer == 'y':
        	self.count_iteration()

    def recognize_order(self):

        self.list_of_files = list(sorted(filter
                                         (lambda x: "scenario" in x,
                                          os.listdir('.'))))

        for element in self.list_of_files:
            element = element.replace('scenario-', "").replace('.txt', "")
            self.logger.debug(element)
            self.logger.debug(datetime.fromtimestamp(int(element)))

        self.logger.info('Files to join: ' + str(self.list_of_files))

    def count_iteration(self):
        dataset = pd.read_csv(
            self.list_of_files[0], delimiter=';', header=0)

        start_iteration = dataset.iloc[-1]["current_iteration"]
        self.list_of_files.pop(0)
        self.logger.debug(start_iteration)

        for element in self.list_of_files:
            self.logger.debug("%s is processed", element)
            self.temp = pd.read_csv(element, delimiter=';', header=0)
            self.temp.loc[:]["current_iteration"] = (self.temp.loc[:]["current_iteration"].apply(
                lambda x: x + start_iteration))
            start_iteration = self.temp.iloc[-1]["current_iteration"]
            self.logger.debug("Next interation: %d", start_iteration)
            dataset = dataset.append(self.temp)

        self.logger.debug(dataset.loc[:]['current_iteration'])
        file_name = "Resulting.txt"
        print("\n \n \n")
        dataset.set_index(keys="unix_time", inplace=True)
        dataset.to_csv(file_name, sep=';')

        self.logger.info("______Data saved as %s _____",file_name)


if __name__ == '__main__':
    JoinFile()
