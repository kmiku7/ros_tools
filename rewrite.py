#!/usr/bin/env python
# encoding: utf-8

import argparse
import rosbag


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-F', dest='bag_filename', required=True,
        help="input bag file.")
    parser.add_argument(
        '-O', dest='out_filename', required=True,
        help="output bag file.")
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    in_bag = rosbag.Bag(args.bag_filename)
    out_bag = rosbag.Bag(args.out_filename, mode='w')
    for topic, msg, t in in_bag.read_messages(raw=True):
        out_bag.write(topic, msg, t, raw=True)
    in_bag.close()
    out_bag.close()

if __name__ == '__main__':
    main()
