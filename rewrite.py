#!/usr/bin/env python
# encoding: utf-8

import argparse
import rosbag
import genpy
import heapq


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-F', dest='bag_filename', required=True, help="input bag file.")
    parser.add_argument(
        '-O', dest='out_filename', required=True, help="output bag file.")
    parser.add_argument(
        '-S',
        dest='start_timestamp',
        default=0,
        type=int,
        help='Start timestamp in seconds. Inclusive.')
    parser.add_argument(
        '-E',
        dest='end_timestamp',
        default=0,
        type=int,
        help='End timestamp in seconds. exclusive.')
    parser.add_argument(
        '-T',
        dest='topics',
        default=[],
        nargs='+',
        type=str,
        help='topic name.')
    return parser


def message_generator(msg_generator, window_size=255):
    msg_wnd = []
    while True:
        while len(msg_wnd) < window_size and msg_generator:
            try:
                topic, msg, t = msg_generator.next()
                offset = msg[1][3]
                heapq.heappush(msg_wnd, (t, offset, topic, msg))
            except StopIteration:
                msg_generator = None
        if len(msg_wnd) <= 0:
            raise StopIteration
        item = heapq.heappop(msg_wnd)
        yield (item[2], item[3], item[0])


def main():
    parser = get_parser()
    args = parser.parse_args()

    in_bag = rosbag.Bag(args.bag_filename)
    out_bag = rosbag.Bag(args.out_filename, mode='w')
    read_options = {'raw': True}
    if args.topics:
        read_options['topics'] = args.topics
    if args.start_timestamp:
        read_options['start_time'] = genpy.Time(args.start_timestamp, 0)
    if args.end_timestamp:
        read_options['end_time'] = genpy.Time(args.end_timestamp - 1,
                                              999999999)
    for topic, msg, ros_ts in message_generator(
            in_bag.read_messages(**read_options)):
        out_bag.write(topic, msg, ros_ts, raw=True)
    in_bag.close()
    out_bag.close()


if __name__ == '__main__':
    main()
