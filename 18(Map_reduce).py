#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep

class MREarliestFatalKernelError(MRJob):

    def parse_log_line(self, line):
        # Split the log line into parts using spaces.
        parts = line.split()
        return parts

    def mapper_find_errors(self, _, line):
        # Parse the log line into parts
        parsed_line = self.parse_log_line(line.strip())
        # will check if the log line has at least 6 parts and contains the phrase "Lustre mount FAILED"
        if len(parsed_line) >= 6 and "Lustre mount FAILED" in line:
            # Extract the date and time from the 5th part (index 4)
            date_time = parsed_line[4]
            # A key-value pair with None as the key and date_time as the value
            yield None, date_time

    def reducer_find_earliest(self, _, date_times):
        # Find the earliest date and time from the list of date_times
        earliest_date_time = min(date_times)
        # Emit the earliest date and time with None as the value
        yield earliest_date_time, None

    def steps(self):
        # Define the steps for the MapReduce job: a single step with a mapper and a reducer
        return [
            MRStep(mapper=self.mapper_find_errors,
                   reducer=self.reducer_find_earliest)
        ]

if __name__ == '__main__':
    # Run the MapReduce job
    MREarliestFatalKernelError.run()