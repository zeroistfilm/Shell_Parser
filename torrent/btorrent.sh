#!/bin/bash
#
# Credit to original author: sam https://github.com/nikzad-avasam/block-torrent-on-server
# GitHub:   https://github.com/nikzad-avasam/block-torrent-on-server
# Author:   sam nikzad

echo -n "Blocking all torrent traffic on your server. Please wait ... "

# Download the list of trackers
if wget -q -O /etc/trackers https://raw.githubusercontent.com/nikzad-avasam/block-torrent-on-server/main/domains; then
    echo "Trackers list downloaded."
else
    echo "Failed to download trackers list."
    exit 1
fi

# Create the denypublic script
cat >/etc/cron.daily/denypublic<<'EOF'
#!/bin/bash
IFS=$'\n'
L=$(/usr/bin/sort /etc/trackers | /usr/bin/uniq)
for fn in $L; do
    /sbin/iptables -D INPUT -d $fn -j DROP
    /sbin/iptables -D FORWARD -d $fn -j DROP
    /sbin/iptables -D OUTPUT -d $fn -j DROP
    /sbin/iptables -A INPUT -d $fn -j DROP
    /sbin/iptables -A FORWARD -d $fn -j DROP
    /sbin/iptables -A OUTPUT -d $fn -j DROP
done
EOF

# Make the denypublic script executable
chmod +x /etc/cron.daily/denypublic

# Download the Thosts file
if curl -s -LO https://raw.githubusercontent.com/nikzad-avasam/block-torrent-on-server/main/Thosts; then
    echo "Thosts file downloaded."
else
    echo "Failed to download Thosts file."
    exit 1
fi

# Append Thosts to /etc/hosts and remove duplicates
cat Thosts >> /etc/hosts
sort -uf /etc/hosts > /etc/hosts.uniq && mv /etc/hosts{.uniq,}

# Clean up
rm -f Thosts

echo "OK"
