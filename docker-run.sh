#!/bin/sh
if [ ! -f /data/config.yaml ]; then
	if ! /usr/bin/mautrix-googlechat -c /data/config.yaml -e; then
		echo "Failed to generate default config file." >&2
		exit 1
	fi
	echo "Didn't find a config file."
	echo "Copied default config file to /data/config.yaml"
	echo "Modify that config file to your liking, then restart the container."
	exit 0
fi
exec /usr/bin/mautrix-googlechat -c /data/config.yaml
