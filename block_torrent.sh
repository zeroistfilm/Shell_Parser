#!/bin/sh
# Block Torrent algo string using Boyer-Moore (bm)
iptables -A FORWARD -m string --algo bm --string "BitTorrent" -j DROP
iptables -A FORWARD -m string --algo bm --string "BitTorrent protocol" -j DROP
iptables -A FORWARD -m string --algo bm --string "peer_id=" -j DROP
iptables -A FORWARD -m string --algo bm --string ".torrent" -j DROP
iptables -A FORWARD -m string --algo bm --string "announce.php?passkey=" -j DROP
iptables -A FORWARD -m string --algo bm --string "torrent" -j DROP
iptables -A FORWARD -m string --algo bm --string "announce" -j DROP
iptables -A FORWARD -m string --algo bm --string "info_hash" -j DROP
iptables -A FORWARD -m string --algo bm --string "/default.ida?" -j DROP
iptables -A FORWARD -m string --algo bm --string ".exe?/c+dir" -j DROP
iptables -A FORWARD -m string --algo bm --string ".exe?/c_tftp" -j DROP
# Block Torrent keys
iptables -A FORWARD -m string --algo kmp --string "peer_id" -j DROP
iptables -A FORWARD -m string --algo kmp --string "BitTorrent" -j DROP
iptables -A FORWARD -m string --algo kmp --string "BitTorrent protocol" -j DROP
iptables -A FORWARD -m string --algo kmp --string "bittorrent-announce" -j DROP
iptables -A FORWARD -m string --algo kmp --string "announce.php?passkey=" -j DROP
# Block Distributed Hash Table (DHT) keywords
iptables -A FORWARD -m string --algo kmp --string "find_node" -j DROP
iptables -A FORWARD -m string --algo kmp --string "info_hash" -j DROP
iptables -A FORWARD -m string --algo kmp --string "get_peers" -j DROP
iptables -A FORWARD -m string --algo kmp --string "announce" -j DROP
iptables -A FORWARD -m string --algo kmp --string "announce_peers" -j DROP
# Block torrent - https://www.digitalocean.com/community/questions/updating-iptables-to-block-torrent-traffic
iptables -A INPUT -m string --string "BitTorrent" --algo bm -j DROP
iptables -A INPUT -m string --string "BitTorrent protocol" --algo bm -j DROP
iptables -A INPUT -m string --string "peer_id=" --algo bm -j DROP
iptables -A INPUT -m string --string ".torrent" --algo bm -j DROP
iptables -A INPUT -m string --string "announce.php?passkey=" --algo bm -j DROP
iptables -A INPUT -m string --string "torrent" --algo bm -j DROP
iptables -A INPUT -m string --string "announce" --algo bm -j DROP
iptables -A INPUT -m string --string "info_hash" --algo bm -j DROP
iptables -A INPUT -m string --string "tracker" --algo bm -j DROP
iptables -A INPUT -m string --string "get_peers" --algo bm -j DROP
iptables -A INPUT -m string --string "announce_peer" --algo bm -j DROP
iptables -A INPUT -m string --string "find_node" --algo bm -j DROP