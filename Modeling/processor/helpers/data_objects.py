import itertools


class ShotsData(object):
    '''
    ShotsData: This object holds some shots data for easier manipulation and
    output.

    Attributes:
    oord: Whether the shots data represents offense or defense.
    size: Data for player size, ie big/small/ALL
    data: A dict with keys of dates, values of dicts containing data
    '''
    def __init__(self, oord, size, data):
        self.oord = oord
        self.size = size
        self.data = data

    def __repr__(self):
        # String representation, shows up with print()
        return '{' + str(self.oord) + ',' + str(self.size) + '}'


class DataVars(object):
    '''
    DataVars: This object holds variables and makes it easier to collect and
    aggregate them. Acts like an ordereddict with extra methods to make
    adding new elements easier.

    Used to collect player_vars, team_vars, game_vars into a row for a single
    game output.

    Attributes:
    - values: a list of all data values
    - header: a list of all labels corresponding to the data values
    - prefix: a string prefix to tack onto header items

    Methods:
    - add_datavars(): Combine other DataVars objects with this one
    - add_lists_of_datavars(): Takes in lists of DataVars objects and
    combines them into one DataVars objects.
    - add_dict(): Takes in a dict of data and adds it into the
    current datavar object
    - add_shots_data(): Takes in a ShotData object and adds it to current object
    '''
    def __init__(self, prefix=''):
        self.values = []
        self.header = []
        self.prefix = prefix

    def __repr__(self):  # string representation
        return str(zip(self.header, self.values))

    def add_datavars(self, *datavars):
        # iterate through DataVars objects received in arguments
        for datavar in datavars:
            # add each DataVars object to the values and headers
            self.values.extend(datavar.values)
            self.header.extend(datavar.header)

    def add_lists_of_datavars(self, *lists_datavars):
        # iterate through arguments
        for list_datavar in lists_datavars:

            # if current arg is a list, add all list elements
            if isinstance(list_datavar, list):
                self.add_datavars(*list_datavar)
            # otherwise if it's a singleton, add that one DataVars object
            else:
                self.add_datavars(list_datavar)

    def add_dict(self, add_dict, data_prefix=''):
        # sort the dict received into a list of sorted tuples
        sorted_items = sorted(add_dict.items())

        # add the values from the dict into self.values
        self.values.extend([item[1] for item in sorted_items])

        # mumbo jumbo to get string prefixes for the header right
        full_prefix = ''
        if self.prefix:
            full_prefix = full_prefix + self.prefix + '_'

        if data_prefix:
            full_prefix = full_prefix + data_prefix + '__'

        # add the keys from the dict into the header with appropriate prefixes
        self.header.extend([full_prefix + str(item[0]) for item in sorted_items])

    def add_shots_data(self, shots_data, date, data_prefix=''):
        # grab the data dict from the ShotsData object
        add_dict = shots_data.data[date]

        # create a string prefix with offense/defense and player size
        shots_prefix = data_prefix + '_' + str(shots_data.oord) + '_' + str(shots_data.size)

        # add the data dict to the current object
        self.add_dict(add_dict, shots_prefix)
