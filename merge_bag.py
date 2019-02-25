#!/usr/bin/env python
# encoding: utf-8

import argparse
import heapq
import rosbag


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-o', dest='output_bag', required=True, help='merged bag filename'
    )
    parser.add_argument(
        '-i', dest='input_bags', required=True, nargs='+', help='bags wanted be merged.'
    )
    return parser

class MsgWrapper(object):
    def __init__(self, topic, msg, ts):
        self._topic = topic
        self._msg = msg
        self._ts = ts
    
    def __cmp__(self, other):
        return self._ts.__cmp__(other.get_ts())
    
    def get_ts(self):
        return self._ts

    def get_original_message(self):
        return self._topic, self._msg, self._ts

class BagReaderWrapper(object):
    def __init__(self, filepath):
        self._bag = rosbag.Bag(filepath, 'r')
    
    def __iter__(self):
        for topic, msg, ts in self._bag.read_messages(raw=True):
            yield MsgWrapper(topic, msg, ts)
    
    def close(self):
        self._bag.close()
        self._bag = None

def main():
    parser = get_parser()
    args = parser.parse_args()

    input_bags = []
    for item in args.input_bags:
        in_bag = BagReaderWrapper(item)
        input_bags.append(in_bag)

    out_bag = rosbag.Bag(args.output_bag, 'w')
    for wrapped_msg in heapq.merge(*input_bags):
        topic, msg, ts = wrapped_msg.get_original_message()
        out_bag.write(topic, msg, ts, raw=True)
    out_bag.close()

    for bag in input_bags:
        bag.close()

if __name__ == '__main__':
    main()