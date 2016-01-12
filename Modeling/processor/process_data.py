from job_handler import JobHandler
from classifier import fit_trees_model

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

    Params:
    - min_year, max_year: range of seasons to process. Full range is 2000-2014
    - history_steps: number of games to calculate a moving average over. also
    the minimum number of games in team histories before game counts
    - min_player_games: minimum number of games played by a player to be
    included in a game history
    - num_players: deprecated. previously was the number of players to take
    from each roster
    - bench_positions: categorizes bench players by category, then takes
    this number from each position to add to the final roster
    - path: filename of output file
    - output_format: takes arguments csv, pkl, or pickle. outputs dataframe
    in corresponding format
    '''

    # initial processing parameters
    base_params = {'min_year': 2003, 'max_year': 2013,
                   'history_steps': 7, 'min_player_games': 2,
                   'num_players': 9, 'path': 'output_3.pkl',
                   'bench_positions': {'Guard': 1, 'Wing': 1, 'Big': 1},
                   'output_format': 'pickle'
                   }

    # perform additional jobs with these parameters changed
    additional_search_params = []

    for var in range(4, 21):
        filename = 'output_' + str(var) + '.pkl'
        additional_search_params.append({'path': filename, 'history_steps': var})

    # create a list of jobs with specified parameters
    jobhandler = JobHandler(base_params, additional_search_params)

    # perform all processing jobs
    jobhandler.process_jobs()

    fit_trees_model(base_params['path'])

    for param_set in additional_search_params:
        fit_trees_model(param_set['path'])
