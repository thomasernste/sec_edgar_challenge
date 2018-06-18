import csv
import time
import datetime
import sys
import functools


def parse_sec_edgar_data(input_data_csv, inactivity_period_file, output_data_csv):

    #Open the file from the first function argument and read it in as a list
    with open(input_data_csv, 'r') as sec_file:
        reader = csv.reader(sec_file)
        sec_input = list(reader)

    #Read in the seconds value from the inactivitiy period file
    with open(inactivity_period_file, 'r') as time_file:
        reader = csv.reader(time_file)
        for row in reader:
            inactivity_value = int(row[0])



    # Preserve only the columns I need from the input data array to complete the data engineering challenge
    # Also, add together the date and time columns to create one date_time column, then delete the date and time columns.
    for row in sec_input:
        row.insert(1, row[1] + '_' + row[2])
        del row[2:]

    #Convert the date_time column to a datetime object, then convert that to the time in UTC seconds
    for row in sec_input[1:]:
        row[1] = datetime.datetime.strptime(row[1], '%Y-%m-%d_%H:%M:%S')
        row[1] = str(int(time.mktime(row[1].timetuple())))

    # sec_input = [['user', 'time', 'address'], ['F', '0', 'a'], ['T', '0', 'b'], ['T', '0', 'c'], ['T', '1', 'd'],
    #         ['B', '1', 'e'], ['K', '2', 'f'], ['J', '2', 'g'], ['T', '3', 'h'], ['J', '4', 'i'], ['B', '4', 'j'],
    #         ['B', '5', 'k']]


    #initialize dictionary
    dct = {}

    #initalize list
    result = []


    for user, seconds in sec_input[1:]:
        #if the user value for a row is equal to some 'ip' address, iterate through all of these ip addresses
        if dct.get(user):
            # If current time is within X (inactivity_value) seconds of last user's time....
            if int(seconds) <= int(dct[user][-1]) + inactivity_value:
                # The time between users is within X (inactivity_value) seconds of last user's time,
                # so the code appends the time value to the dictionary for this user
                dct[user].append(int(seconds))
                # Skip the rest of this iteration
                continue
            # Skips here if there is not another previous request by this user within the time in the
            # inactivity value table, so it creates a temporary list t with the data for this visit
            t = dct[user]
            # Appends the t list from above to the final result list
            result.append([user, t[0], t[-1], (t[-1] - t[0] + 1), len(t)])

        # Overwrites user in the dct with new data
        dct[user] = [int(seconds)]

        # Parses the rest of the dct here, appending each dictionary key and value to the list that have
        # not already been appended to the list
        # Also, the formula at index 3 in the list is the calculation that produces the duration column.
        b = [[k, v[0], v[-1], (v[-1] - v[0] + 1), len(v)] for k, v in
             dct.items()]

    # header = [["ip", "start_time", "end_time", "duration", "count"]]

    # Combines the previous result list that contains visits such as ip address where the user visited the
    # site and then made at least one more visit after the inactivity value expires. The code appends the early
    # visits by such a user before clearing the dictionary for the follow-up visit by the user after the
    # inactivity expiration time.
    result = sorted(result + b, key=lambda x: x[1:3])

    # Because I iterated through users rather than  by seconds, the rows in the 2d 'result' array at this
    # time are not sorted properly. The conditional_sort function sorts first in descending order by the
    # start_time. But in unique conditions, the rows are conditionally sorted in descending order by end_
    # time. Specifically, this function adds a condition that if -- for example -- we have two ip addresses,
    # and the first has an earlier start_time than the second, but the second has an end_time that occurs
    # before the expiration of the inactivity value for the first ip address, then the second ip address
    # will appear earlier in the output in such conditions.

    def conditional_sort(ip1, ip2):
        start = 1
        end = 2

        if ip1[start] - ip2[start] >= inactivity_value:
            return ip1[start] - ip2[start]

        else:
            return ip1[end] - ip2[end]

    result = [result[0]] + sorted(result[1:], key=functools.cmp_to_key(conditional_sort))

    # This loop converts the UTC seconds value for the date_time columns back to the datetime format.
    for row in result:
        row[1] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[1]))
        row[2] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(row[2]))


    # This code writes the data one row at a time to the csv
    with open(output_data_csv, 'w') as myfile:
        wr = csv.writer(myfile)
        for i in result:
            wr.writerow(i)

def main():
    input_data_csv = sys.argv[1]
    inactivity_period_file = sys.argv[2]
    output_data_csv = sys.argv[3]
    parse_sec_edgar_data(input_data_csv, inactivity_period_file, output_data_csv)

if __name__ == '__main__':
	main()