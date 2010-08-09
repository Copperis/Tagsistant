#!/usr/bin/perl

#
# test script for tagsistant
#
# using perl threads, the script starts a tagsistant instance
# on a thread, fetching all the output and feeding it to a
# file. on the main thread the script performs a sequence of
# tests and reports any error on STDERR
#

use strict;
use warnings;

use threads;
use threads::shared;

use Errno;
use POSIX;

my $BIN = "./tagsistant";
my $MPOINT = "$ENV{HOME}/tags";
my $REPOSITORY = "$ENV{HOME}/.tagsistant_test";
my $MCMD = "$BIN -d -v --repository=$REPOSITORY $MPOINT 2>&1";
my $UMCMD = "/usr/bin/fusermount -u $MPOINT";
my $TID = undef;
my $tc = 0;
my $tc_ok = 0;
my $tc_error = 0;
my $output = undef;

start_tagsistant();

print "Doing tests...\n";

# ---------[tagsistant is mounted, do tests]---------------------------- <---

test("ls -a $MPOINT");
test("ls -a $MPOINT/tags");
out_test('^\.$', '^\.\.$');
test("ls -a $MPOINT/archive");
test("ls -a $MPOINT/stats");
test("ls -a $MPOINT/relations");

# ---------[no more test to run]---------------------------------------- <---

print "\nTests done! $tc test run - $tc_ok test succeeded - $tc_error test failed\n";

stop_tagsistant();
$TID->join();

exit();

# ---------[script end, subroutines follow]-----------------------------

#
# the core of the thread running tagistant, invoked
# by start_tagsistant()
#
sub run_tagsistant {
	open(TS, "$MCMD|") or die("Can't start tagsistant ($MCMD)\n");
	open(LOG, ">/tmp/tagsistant.log") or die("Can't open log file /tmp/tagsistant.log\n");

	while (<TS>) {
		print LOG $_ if /^TS/;
	}

	close(LOG);
	threads->exit();
}

#
# prepare the testbad
#
sub start_tagsistant {
	print "Creating the testbed...\n";

	#
	# remove old copy of the repository
	# each run must start from zero!
	#
	system("rm -rf $REPOSITORY 1>/dev/null 2>/dev/null");
	die("Can't remove $REPOSITORY\n") unless $? == 0;

	#
	# start tagsistant in a separate thread
	#
	$TID = threads->create(\&run_tagsistant);

	#
	# wait until tagsistant is brought to life
	#
	sleep(1);

	#
	# check if thread was properly started
	#
	unless (defined $TID and $TID) {
		die("Can't create tagsistant thread!\n");
	}
	print "Testbed running!\n";
}

sub stop_tagsistant {
	system($UMCMD);
}

#
# Execute a command and check its exit code
#
sub test {
	$tc++;

	#
	# build the command
	#
	my $command = join(" ", @_);

	#
	# run the command and trap the output in a global variable
	#
	$output = qx|$command 2>&1|;

	#
	# guess the operation status (OK or ERROR!)
	#
	my $status = ($? == 0) ? "[  OK  ]" : "[ERROR!]";

	#
	# print summary
	#
	print "\n.... $status [#$tc] $command ", "." x (60 - length($tc) - length($command)), "cmd..\n\n$output";

	#
	# return 0 if everything went OK
	#
	if ($? == 0) {
		$tc_ok++;
		return 0;
	}

	#
	# report errors
	#
	my $signal = $? & 127;
	my $coredump = $? & 128;
	my $exitstatus = $? >> 8;
	my $strerror = strerror($exitstatus);

	if ($? == -1) {
		print " *** failed to execute: $!\n";
	} elsif ($signal) {
		printf " *** child died with signal %d, %s coredump\n", ($signal),  ($coredump) ? 'with' : 'without';
	} else {
		printf " *** child exited with value %d: %s\n", $exitstatus, $strerror;
	}

	$tc_error++;
	return $exitstatus;
}

#
# apply a list of regular expressions on the output
# of last performed command
#
sub out_test {
	my $stc = 0;
	my $status = undef;
	for my $rx (@_) {
		$tc++;
		$stc++;
		unless ($output =~ m/$rx/m) {
			$tc_error++;
			$status = "[ERROR!]";
		} else {
			$tc_ok++;
			$status = "[  OK  ]";
		}
		print "\n \\.. $status [#$tc.$stc] /$rx/ ", "." x (56 - length($tc) - length($stc) - length($rx)), "..re..\n";
	}

	return 0;
}
