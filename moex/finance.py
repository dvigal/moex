import math

def pv(present_value, percent, time_unit):
    '''
    present discounted value
    Pn / (1 - r) ** n
    :return:
    '''

    return present_value / (1 + percent) ** time_unit


def pv2(present_value, current_value, percent):

    return (present_value / current_value - 1) / percent


def yield_of_period(present_value, init_value):

    return present_value / init_value - 1

def yield_per_year(present_value, init_value, year):

    return math.sqrt(present_value / init_value - 1)
