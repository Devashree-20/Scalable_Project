#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRKERNRTSPEvents(MRJob):

    def parse_log_line(self, line):
        # Split a line of the log file into parts separated by spaces
        parts = line.split()
        return parts

    def mapper_count_kernrtsp(self, _, line):
        # Parse each line of the log file
        parsed_line = self.parse_log_line(line.strip())
        # Check if the line has enough parts and contains 'KERNRTSP'
        if len(parsed_line) >= 6 and 'KERNRTSP' in parsed_line:
            # Extract the node information (assuming it's the 4th column)
            node = parsed_line[3]
            # Emit the node with a count of 1
            yield node, 1

    def reducer_sum_counts(self, node, counts):
        # Sum all counts for each node
        yield None, (sum(counts), node)

    def reducer_find_top(self, _, node_count_pairs):
        # Find the node with the highest count of KERNRTSP events
        top_node = max(node_count_pairs)
        yield top_node

    def steps(self):
        # Define the steps of the MapReduce job
        return [
            MRStep(mapper=self.mapper_count_kernrtsp,
                   reducer=self.reducer_sum_counts),
            MRStep(reducer=self.reducer_find_top)
        ]

if __name__ == '__main__':
    # Run the MapReduce job
    MRKERNRTSPEvents.run()