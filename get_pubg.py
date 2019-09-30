#!/usr/bin/env python
# coding: utf-8


import requests
import json
import threading
from queue import Queue
import time
import os


lock_player = threading.Lock()
lock_match = threading.Lock()
api_key = "### PUT YOUR PUBG API KEY HERE ###"


class players_matches(object):

    def __init__(self, data=[], flag='p'):
        self.queue = Queue()
        self.flag = flag
        if self.flag == 'p':
            # could be either p for players or m for matches
            self.seen = {}
        else:
            self.seen = set()
        self.failed = set()
        for d in data:
            self.queue.put(d)
            if self.flag == 'p':
                self.seen[d] = time.time()
            else:
                self.seen.add(d)



class crawler_match(threading.Thread):

    def __init__(self, player_data, match_data, shards='psn', match_save_loc='match',
                 telemetry_save_loc='telemetry', *args, **kwargs):
        super(crawler_match, self).__init__(*args, **kwargs)
        self.player_data = player_data
        self.match_data = match_data
        self.shards = shards
        self.init_pubg_api()
        self.match_save_loc = match_save_loc
        self.telemetry_save_loc = telemetry_save_loc

    def init_pubg_api(self):
        self.url = "https://api.pubg.com/shards/%s/matches/" % self.shards
        self.header = {"Accept": "application/vnd.api+json"
                       }

    def run(self):
        fail_count = 0
        while True:
            if self.match_data.queue.empty():
                fail_count += 1
                if fail_count == 10:
                    break
                time.sleep(6)
                continue
            curr_match = self.match_data.queue.get()
            try:
                players = self.get_players(curr_match)
            except (KeyError, IndexError, json.JSONDecodeError) as e:
                if curr_match not in self.match_data.failed:
                    self.match_data.failed.add(curr_match)
                    self.match_data.queue.put(curr_match)
                continue
            fail_count = 0
            for p in players:
                if (p not in self.player_data.seen or
                   time.time() - self.player_data.seen[p] >= 86400 * 2):
                        self.player_data.queue.put(p)
                        with lock_player:
                            self.player_data.seen[p] = time.time()
            time.sleep(1)

    def get_players(self, m):
        r = requests.get(self.url + m, headers=self.header)
        match_json = json.loads(r.content)
        with open(os.path.join(self.match_save_loc, '%s.json' % m), 'w') as fp:
            json.dump(match_json, fp)
        r.connection.close()
        r.close()
        player_info = [x['attributes']['stats'] for x in
                       match_json['included'] if x['type'] == 'participant']
        telemetry_url = [x for x in match_json['included']
                         if x['type'] == 'asset'][0]['attributes']['URL']
        r = requests.get(telemetry_url, headers=self.header)
        telemetry_json = json.loads(r.content)
        with open(os.path.join(self.telemetry_save_loc, '%s.json' % m), 'w') as fp:
            json.dump(telemetry_json, fp)
        r.connection.close()
        print('Saved Match Info %s at %s' % (m, time.asctime(time.localtime())))
        return [x['playerId'] for x in player_info]

    @staticmethod
    def get_samples(shards='psn'):
        url = 'https://api.pubg.com/shards/%s/samples' % shards
        header = {"Authorization": api_key,
                  "Accept": "application/vnd.api+json"
                  }

        r = requests.get(url, headers=header)
        sample = json.loads(r.content)
        r.connection.close()
        sample_info = sample['data']['relationships']['matches']['data']
        return [x['id'] for x in sample_info]


class crawler_player(threading.Thread):

    def __init__(self, player_data, match_data, shards='psn',
                 player_save_loc='player', *args, **kwargs):
        super(crawler_player, self).__init__(*args, **kwargs)
        self.player_data = player_data
        self.match_data = match_data
        self.shards = shards
        self.init_pubg_api()
        self.player_save_loc = player_save_loc

    def init_pubg_api(self):
        self.url = ("https://api.pubg.com/shards/%s/players?filter[playerIds]="
                    % self.shards)
        self.header = {"Authorization": api_key,
                       "Accept": "application/vnd.api+json"
                       }

    def run(self):
        fail_count = 0
        while True:
            if self.player_data.queue.empty():
                fail_count += 1
                if fail_count == 10:
                    break
                time.sleep(6)
                continue
            curr_player = self.player_data.queue.get()
            try:
                matches = self.get_matches(curr_player)
            except (KeyError, IndexError, json.JSONDecodeError) as e:
                if curr_player not in self.player_data.failed:
                    self.player_data.failed.add(curr_player)
                    self.player_data.queue.put(curr_player)
                continue
            fail_count = 0
            for m in matches:
                if m not in self.match_data.seen:
                    self.match_data.queue.put(m)
                    with lock_match:
                        self.match_data.seen.add(m)
            time.sleep(6)

    def get_matches(self, p):
        r = requests.get(self.url + p, headers=self.header)
        player_json = json.loads(r.content)
        with open(os.path.join(self.player_save_loc, '%s.json' % p),
                  'w') as fp:
            json.dump(player_json, fp)
        r.connection.close()
        match_info = player_json['data'][0]['relationships']['matches']['data']
        print('Saved Player Info %s at %s' % (p, time.asctime(time.localtime())))
        return [x['id'] for x in match_info]


if __name__ == '__main__':
    shards = 'psn'
    player_data = players_matches(flag='p')
    match_data = players_matches(data=crawler_match.get_samples(shards=shards),
                                 flag='m')
    t1 = crawler_match(player_data, match_data, shards=shards)
    t2 = crawler_player(player_data, match_data, shards=shards)
    t1.start()
    t2.start()
