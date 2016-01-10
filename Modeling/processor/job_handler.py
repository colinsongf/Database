import pandas as pd
import numpy as np
import time
from helpers.loaders import Loader, Slicer
from helpers.processors import SeasonProcessor, BoxscoreDataProcessor, ShotsDataProcessor, GameProcessor
from helpers.df_manipulators import RollingCalculator
from objects import Season


class JobHandler(object):
    '''
    JobHandler: This class creates a series of processing jobs.
    Each processing job represents one pass through all specified seasons.

    The class initializes with base_params to use for the first job. Optionally,
    you can specify additional_search_params. This creates multiple jobs with
    different parameters, which can be used to do a parameter search.

    Fuctionality works like this:
    1. Makes list of params to use for each job.
    2. Based on those params, makes list of Processor objects for each job.
    3. Iterates through the list of jobs.
    4. For each job, creates a series of Season objects for the specified
    seasons and processes them.
    5. Outputs to a pandas dataframe with path specified in parameters.

    For usage of additional_search_params, see process_data.py.

    Attributes:
    - base_params: Full set of parameters to use on initial processing job.
    - additional_search_params: A list of dicts. Each dict contains params to
    alter from the base set of params. Each dict represents an additional job.

    - param_list: A constructed list of params to use for each job.
    - processor_list: A constructed list of Processor objects for each job.

    Methods:
    - create_processor_set(): Creates a set of processors to use for a given
    job, given the params specified for that job.
    - create_param_list(): based on the instantiating arguments, creates
    a list of params. Each element is a dict of params, corresponding to
    one job.
    - create_processor_list(): Based on the param list for each job,
    creates a tuple of Processor objects to be used in that job. Puts these
    tuples together in a list, with each element corresponding to a job.

    - process_jobs(): iterates through lists of params and Processor objects
    and performs the specified number of jobs.
    - perform_job(): Given a set of parameters, perform a job
    '''
    def __init__(self, base_params, additional_search_params=[]):
        # initialize params
        self.base_params = base_params
        self.additional_search_params = additional_search_params

        # create a set of params for each job
        self.create_param_list()

        # create a set of Processor objects for each job
        self.create_processor_list()

    def create_param_list(self):
        '''
        create_param_list: based on the instantiating arguments, creates
        a list of params. Each element is a dict of params, corresponding to
        one job.
        '''
        # initialize list and add the initial job
        param_list = []
        param_list.append(self.base_params)

        # iterate through the jobs specified in additional_search_params
        for param_set in self.additional_search_params:
            if param_set:  # check if param set is not blank

                # combine base and additional search params
                search = self.base_params.copy()
                search.update(param_set)

                # add params for another job to param list
                param_list.append(search)

        # set param list containing all params for all jobs
        self.param_list = param_list

    def create_processor_list(self):
        processor_list = []

        # create a tuple of processors for each job specified in the param_list
        for param_set in self.param_list:
            processor_list.append(self.create_processor_set(param_set))

        # return list containing tuples of processors to use for each job
        self.processor_list = processor_list

    def process_jobs(self):
        # get total number of jobs
        num_jobs = len(self.processor_list)
        print 'Number of Jobs: ' + str(num_jobs)
        start_time = time.time()  # timer function

        # load dataframes that span multiple seasons
        large_dfs = Loader.load_large_dfs()

        # iterate through jobs
        for processors, params, num in zip(self.processor_list, self.param_list, range(1, num_jobs + 1)):
            print 'Starting Job #' + str(num) + ': ' + str(params['path'])

            # perform job with specified params and processor objects
            self.perform_job(large_dfs, params['min_year'], params['max_year'], processors, params['path'])

        print "ALL JOBS FINISHED"
        print("Total time: --- %s seconds ---" % (time.time() - start_time))

    @staticmethod
    def perform_job(large_dfs, min_year, max_year, processors, path):
        '''
        perform_job: Iterates through all seasons in a specified job. Collects
        data with one row for each game into final_output, a list of lists.
        Then converts this list of lists into a pandas dataframe and dumps
        it to disk.
        '''
        job_time = time.time()  # timer function

        # initialize final_output
        final_output = []

        # iterate through seasons
        for year in range(min_year, max_year + 1):
            checkpoint_time = time.time()  # timer to process each season

            # initialize season object
            season = Season(year, large_dfs, processors)

            # process global stats
            season.process_globals()

            # create player and team objects
            season.create_objects()

            # process data within player and team objects
            season.process_objects()

            # creates game objects and populates them with data
            season.create_game_objects()

            # create data rows for all games in a season
            final_header, season_output = season.generate_output()

            # add data for current season to final output
            final_output.extend(season_output)

            print 'Completed Season ' + str(year)
            print("Time: --- %s seconds ---" % (time.time() - checkpoint_time))

        # form a pandas dataframe with the data
        df_final_data = pd.DataFrame.from_records(final_output, columns=final_header)
        print df_final_data.shape

        # write to disk
        df_final_data.to_csv('./output/' + path)
        print 'Job Finished: ' + str(path)
        print("Job time: --- %s seconds ---" % (time.time() - job_time))

    @staticmethod
    def create_processor_set(params):
        '''
        create_processor_set: Given a list of params, create Processor objects
        '''
        roller = RollingCalculator(params['history_steps'], params['min_player_games'])

        processors = (BoxscoreDataProcessor(roller), ShotsDataProcessor(roller), GameProcessor(params['history_steps'], params['min_player_games'], params['num_players'], params['bench_positions']))

        return processors
