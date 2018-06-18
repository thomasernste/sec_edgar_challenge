import csv
import time
import datetime



def parse_sec_edgar_data(input_data_csv, inactivity_period_file, output_data_csv):

    sec_file = open(input_data_csv, 'r')

    with open(input_data_csv, 'r') as sec_file:
        reader = csv.reader(sec_file)
        sec_input = list(reader)

    with open(inactivity_period_file, 'r') as time_file:
        reader = csv.reader(time_file)
        for row in reader:
            inactivity_value = int(row[0])

    for row in sec_input:
        row.insert(1, row[1] + '_' + row[2])
        del row[2:]


    for row in sec_input:
        row[1] = row[1]

    for row in sec_input[1:]:
        row[1] = datetime.datetime.strptime(row[1], '%Y-%m-%d_%H:%M:%S')
        row[1] = str(int(time.mktime(row[1].timetuple())))


    dct = {}

    result = []
    for user, seconds in sec_input[1:]:
        if dct.get(user):
            # If current time is within X (inactivity_value) seconds of last user's time
            if int(seconds) <= int(seconds) + inactivity_value:
                dct[user].append(int(seconds))
                # Skip the rest of this iteration
                continue

            t = dct[user]
            # Append user data from dct to result
            result.append([user, t[0], t[-1], len(t)])

        # Overwrite user in the buffer dct with new data
        dct[user] = [int(seconds)]


    # Parse the rest of the buffer
        b = [[k, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v[0])),
              time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(v[-1])), (v[-1] - v[0] + 1), len(v)] for k, v in
             dct.items()]

    # header = [["user_id", "start_time", "end_time", "count"]]
    result = sorted(result + b, key=lambda x: x[1:3])

    with open(output_data_csv, 'w') as myfile:
        wr = csv.writer(myfile)
        wr.writerow(result)