from job_handler import JobHandler

if __name__ == "__main__":

    '''
    Usage: Set all params for the initial processing job in base_params.

    To do additional processing jobs with other parameters, use additional_search_params.

    additional_search_params: a list of dictionaries. Each item in the list
    is a dictionary, each item represents one additional job.
    This dictionary contains parameters to change FROM THE BASE PARAMS (not from
    the previous search). No need to specify all params, only ones to change.

    additional_search_params = [] if no additional jobs

    Note: Change the path param from base or you will overwrite the initial job.
    '''

    # initial processing parameters
    base_params = {'min_year': 2003, 'max_year': 2013,
                   'history_steps': 8, 'min_player_games': 2,
                   'num_players': 9, 'path': 'output.csv'}

    # perform additional jobs with these parameters changed
    additional_search_params = []

    # create a list of jobs with specified parameters
    jobhandler = JobHandler(base_params, additional_search_params)

    # perform all processing jobs
    jobhandler.process_jobs()
