#
# Regular cron jobs for the tagsistant package
#
0 4	* * *	root	[ -x /usr/bin/tagsistant_maintenance ] && /usr/bin/tagsistant_maintenance
