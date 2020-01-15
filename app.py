import pandas
import pandasql
import csv
import datetime


def num_rainy_days(filename):
    weather_data = pandas.read_csv(filename)
    q = """
        SELECT
        count(rain)
        FROM
        weather_data
        where rain = 1
        group by
        rain;
        """
    # Execute your SQL command against the pandas frame
    rainy_days = pandasql.sqldf(q.lower(), locals())
    return rainy_days


def max_temp_aggregate_by_fog(filename):

    weather_data = pandas.read_csv(filename)

    q = """
    SELECT 
    fog, max(maxtempi)
    FROM
    weather_data
    GROUP BY
    fog;
    """

    # Execute your SQL command against the pandas frame
    foggy_days = pandasql.sqldf(q.lower(), locals())
    return foggy_days


def avg_weekend_temperature(filename):
    weather_data = pandas.read_csv(filename)

    q = """
    SELECT
    avg(cast(meantempi as integer))
    FROM
    weather_data
    WHERE
    cast (strftime('%w', date) as integer) = 0
    OR
    cast (strftime('%w', date) as integer) = 6;
    """

    # Execute your SQL command against the pandas frame
    mean_temp_weekends = pandasql.sqldf(q.lower(), locals())
    return mean_temp_weekends


def avg_min_temperature(filename):

    weather_data = pandas.read_csv(filename)

    q = """
    SELECT
    avg(cast(mintempi as integer))
    FROM
    weather_data
    WHERE
    rain = 1
    AND
    mintempi > 55;
    """

    # Execute your SQL command against the pandas frame
    avg_min_temp_rainy = pandasql.sqldf(q.lower(), locals())
    return avg_min_temp_rainy


def fix_turnstile_data(filenames):
    for name in filenames:
        with open(name, 'rt') as f:
            result = []
            reader = csv.reader(f)
            for row in reader:
                counter = 0
                while (counter < len(row)-3):
                    result.append(row[0:3]+row[(3+counter):(8+counter)])
                    counter = counter+5
            with open('updated_'+name, 'wt') as m:
                writer = csv.writer(m)
                writer.writerows(result)


def create_master_turnstile_file(filenames, output_file):
    with open(output_file, 'w') as master_file:
        master_file.write('C/A,UNIT,SCP,DATEn,TIMEn,DESCn,ENTRIESn,EXITSn\n')
        for filename in filenames:
            with open(filename, 'r') as input_file:
                for line in input_file:
                    master_file.write(line)


def filter_by_regular(filename):
    turnstile_data = pandas.read_csv(filename)
    turnstile_data = turnstile_data[turnstile_data['DESCn'] == 'REGULAR']
    return turnstile_data


def get_hourly_entries(df):
    df['ENTRIESn_hourly'] = pandas.Series(
        (df['ENTRIESn'] - df['ENTRIESn'].shift(1)).fillna(1), index=df.index)
    return df


def get_hourly_exits(df):
    df['EXITSn_hourly'] = pandas.Series(
        (df['EXITSn'] - df['EXITSn'].shift(1)).fillna(0), index=df.index)
    return df


def time_to_hour(time):
    hour = int(time.split(':')[0])
    return hour


def reformat_subway_dates(date):
    date_formatted = datetime.date.strftime(
        datetime.datetime.strptime(date, '%m-%d-%y'), '%Y-%m-%d')
    return date_formatted


if __name__ == "__main__":
    file = './weather-underground.csv'
    print(num_rainy_days(file))
    print(max_temp_aggregate_by_fog(file))
    print(avg_weekend_temperature(file))
    print(avg_min_temperature(file))

    # turnstile data
    turnstile_data_files = ['turnstile_110507.txt']
    fix_turnstile_data(turnstile_data_files)
