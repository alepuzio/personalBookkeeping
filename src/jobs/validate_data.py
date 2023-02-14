import datetime


class ValidatePeriod:
     def __init__(self, new_read_period):
        self.read_date = new_read_period
    
    def single_date(self):
        x = self.read_date.split()
        first_date = x[0].split('-')[0]
        return ' '.join (first_date, x[1], x[2])

class ValidateDate:
    def __init__(self, new_read_date):
        self.read_date = new_read_date

        
    def month(self):
        self.read_date;
        ## transform in 2021-12-31
        day,month,year = datetime.strptime(self.read_date, '%d %B %Y')
        return datetime.date(2010, 6, 16).isocalendar()[1]
