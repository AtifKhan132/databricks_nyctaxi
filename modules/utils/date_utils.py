from datetime import date
from dateutil.relativedelta import relativedelta

def get_target_yyyymm(months_ago=2):
    """ 
    Returns the year-month string (yyyy-MM) for the given number of months ago
    """
    target_date = date.today() - relativedelta(months=months_ago)
    return target_date.strftime('%y-%m')

def get_months_start_n_months_ago(months_ago: int = 2):
    """
    Returns the date representing the firstday of the month, 'n' months ago
    """
    return date.today().replace(day=1) - relativedelta(months = months_ago)