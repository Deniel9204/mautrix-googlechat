#!/bin/sh
if [ ! -f /data/config.yaml ]; then
	/usr/bin/mautrix-googlechat -c /data/config.yaml -e
	echo "Didn't find a config file."
	echo "Copied default config file to /data/config.yaml"
	echo "Modify that config file to your liking, then restart the container."
	exit
fi
exec /usr/bin/mautrix-googlechat -c /data/config.yaml
