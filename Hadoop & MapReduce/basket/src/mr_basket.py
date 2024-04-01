#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol


class MRBasket(MRJob):
    """
    A class to count item co-occurrence in shopping baskets
    """
    OUTPUT_PROTOCOL = RawProtocol

    def mapper_get_session_items(self, _, line):
        """

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) pairs
        """

        #get a list of items in the same basket of user with that ID
        ID, Date, Items = line.split(',')
        yield((ID, Date), Items)

    def reducer_get_item_pairs(self, key, values):
        distinct_item = list(set(values))
        #reduce to distinct pair consisting two items
        for i, item1 in enumerate(distinct_item):
            for item2 in distinct_item[i + 1:]:
                yield((item1, item2), 1)
                yield((item2, item1), 1)

    def combiner_count_pairs(self, key, values):
        yield (key, sum(values))

    def reducer_count_pairs(self, key, values):
        #reduce to distinct pairs with count
        sum_pairs = sum(values)
        output = f"{key[0]}, [{key[1]}, {sum_pairs}]"
        yield None, output

    def steps(self):
        #use two mapReduce, the second one only requires reducer
        return [
            MRStep(
                mapper=self.mapper_get_session_items,
                reducer=self.reducer_get_item_pairs,
            ),
            MRStep(combiner=self.combiner_count_pairs,
                   reducer=self.reducer_count_pairs)
        ]


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
