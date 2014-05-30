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

my $driver = undef;

#
# get C MACRO TAGSISTANT_INODE_DELIMITER
#
my $tagsistant_id_delimiter = qx(grep TAGSISTANT_INODE_DELIMITER tagsistant.h | cut -f3 -d ' ');
chomp $tagsistant_id_delimiter;
$tagsistant_id_delimiter =~ s/"//g;

if (defined $ARGV[0]) {
	if ($ARGV[0] eq "--mysql") {
		$driver = "mysql";
		system("echo 'drop table objects; drop table tags; drop table tagging; drop table relations;' | mysql -u tagsistant_test_suite --password='tagsistant_test' tagsistant_test");
	} elsif (($ARGV[0] eq "--sqlite") || ($ARGV[0] eq "--sqlite3")) {
		$driver = "sqlite3";
	} else {
		die("Please specify --mysql or --sqlite3");
	}
} else {
	die("Please specify --mysql or --sqlite3");
}

my $FUSE_GROUP = "fuse";

print "Testing with $driver driver\n";

# mount command
my $BIN = "./tagsistant";
my $MPOINT = "/tmp/tagsistant_test_suite";
my $MP = $MPOINT;
my $REPOSITORY = "$ENV{HOME}/.tagsistant_test_suite";
my $MCMD = "$BIN -s -d --debug=s -v --repository=$REPOSITORY ";
if ($driver eq "mysql") {
	$MCMD .= "--db=mysql:localhost:tagsistant_test_suite:tagsistant_test:tagsistant_test";
} elsif ($driver eq "sqlite3") {
	$MCMD .= "--db=sqlite3";
} else {
	die("No driver!");
}
$MCMD .= " $MPOINT 2>&1";

# umount command
my $FUSERMOUNT = `which fusermount` || die("No fusermount found!\n");
chomp $FUSERMOUNT;
my $UMCMD = "$FUSERMOUNT -u $MPOINT";

# other global vars
my $TID = undef;
my $tc = 0;
my $tc_ok = 0;
my $tc_error = 0;
my $output = undef;
my $error_stack = "";

# here we create a garbage file and use it as a test dummy
# the file gets copied and linked inside multiple directories
# and than read again using diff. this ensures proper operations
# on open(), read(), write(), symlink() and readlink()
system("dmesg | tail > /tmp/file");
system("md5sum /tmp/file > /tmp/file2");
system("md5sum /tmp/file2 > /tmp/file3");
system("md5sum /tmp/file3 > /tmp/file4");
system("md5sum /tmp/file4 > /tmp/file5");
system("md5sum /tmp/file5 > /tmp/file6");
system("md5sum /tmp/file6 > /tmp/file7");
system("md5sum /tmp/file7 > /tmp/file8");
system("md5sum /tmp/file8 > /tmp/file9");
system("md5sum /tmp/file9 > /tmp/file10");
system("md5sum /tmp/file10 > /tmp/file11");

start_tagsistant();

# ---------[tagsistant is mounted, do tests]---------------------------- <---

my $testbed_ok = !test("ls -a $MP");

unless ($testbed_ok) {
	print "Testbed not ok!\n";
	goto EXITSUITE;
}

$testbed_ok = !test("ls -a $MP/tags");

unless ($testbed_ok) {
	print "Testbed not ok!\n";
	goto EXITSUITE;
}

out_test('^\.$', '^\.\.$');
test("ls -a $MP/archive");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/stats");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/relations");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/alias");
out_test('^\.$', '^\.\.$');
test("ls -a $MP/store");
out_test('^\.$', '^\.\.$');

# first we create some directories
test("mkdir $MP/store/tag1");
test("touch $MP/store/tag1");
test("mkdir $MP/tags/tag2");
test("stat $MP/store/tag2");
test("stat $MP/tags/tag1");
test("mkdir $MP/store/tag3");
test("mkdir $MP/store/toberenamed");
test("mkdir $MP/store/tobedeleted");
test("ls $MP/tags");
test("ls $MP/archive");
test("stat $MP/store/tag1");
test("ls $MP/store/tag1");

# then we check rename and remove of tag directories
test("mv $MP/store/toberenamed $MP/store/renamed");
test("rmdir $MP/store/tobedeleted");

test("ls $MP/store/");
test("ls $MP/store/tag1");
test("ls $MP/store/tag1/@");
test("ls $MP/store/tag1/@@");

test("cp /tmp/file $MP/store/tag1/@/");
test("cp /tmp/file $MP/store/tag2/@/");
test("stat $MP/store/tag1/@/file");
test("stat $MP/store/tag2/@/file");
test("stat $MP/store/tag1/tag2/@/file");
test("ln -s /tmp/file $MP/store/tag3/@");
test("readlink $MP/store/tag3/@/file");

test("ls -la $MP/archive/");
test("ls -la $MP/store/tag1/@");
test("ls -la $MP/store/tag1/+/tag2/@");
test("ls -la $MP/store/tag1/tag2/@");
test("diff $MP/store/tag1/@/file $MP/store/tag2/@/file");
test("diff $MP/store/tag1/@/file $MP/store/tag2/@/file");

# then we rename a file
test("mv $MP/store/tag1/@/file $MP/store/tag1/@/file_renamed");
test("ls -la $MP/store/tag1/@/");
test("stat $MP/store/tag1/@/file_renamed");

# then we rename a file out of a directory into another;
# that means, from a tagsistant point of view, untag the
# file with all the tag contained in original path and
# tag it with all the tags contained in the destination
# path
test("stat $MP/store/tag2/@/file_renamed");
test("mv $MP/store/tag2/@/file_renamed $MP/store/tag1/@/file");
test("ls -la $MP/store/tag2/@");
test("ls -la $MP/store/tag1/@");
test("ls -la $MP/store/tag3/@/");
test("ls -la $MP/store/tag1/tag3/@/");

# now we check the unlink() method. first we copy two
# files in a tag directory. then we delete the first
# from the directory and the second from the archive/.
test("cp /tmp/file2 $MP/store/tag1/@/tobedeleted");
test("cp /tmp/file3 $MP/store/tag2/@/tobedeleted_fromarchive");
test("rm $MP/store/tag1/@/tobedeleted");
test("ls -l $MP/store/tag1/@/tobedeleted", 2); # we specify 2 as exit status 'cause we don't expect to find what we are searching

# now we create a file in two directories and than
# we delete if from just one. we expect the file to
# be still available in the other. then we delete
# from the second and last one and we expect it to
# desappear from the archive/ as well.
test("cp /tmp/file4 $MP/store/tag1/tag2/@/multifile");
test("stat $MP/store/tag1/@/multifile");
test("stat $MP/store/tag2/@/multifile");
test("rm $MP/store/tag2/@/multifile");
test("ls -l $MP/store/tag1/@/multifile", 0); # 0! we DO expect the file to be here
test("diff /tmp/file4 $MP/store/tag1/@/multifile");
test("diff /tmp/file4 `find $MP/archive/|grep multifile`");
test("rm $MP/store/tag1/@/multifile");
test("ls -l $MP/store/tag1/@/multifile", 2); # 2! we DON'T expect the file to be here

# truncate() test
test("cp /tmp/file5 $MP/store/tag1/@/truncate1");
test("truncate -s 0 $MP/store/tag1/@/truncate1");
test("stat $MP/store/tag1/@/truncate1");
out_test('Size: 0');
test("cp /tmp/file6 $MP/store/tag2/@/truncate2");
test("truncate -s 10 $MP/store/tag2/@/truncate2");
test("stat $MP/store/tag2/@/truncate2");
out_test('Size: 10');

test("mkdir $MP/store/time:/");
test("mkdir $MP/store/time:/year/");
test("mkdir $MP/store/time:/year/eq/2000");
test("cp /tmp/file7 $MP/store/time:/year/eq/2000/@@");
test("stat $MP/store/time:/year/eq/2000/@@/file7");

test("mkdir $MP/store/time:/year/eq/2010");
test("cp /tmp/file8 $MP/store/time:/year/eq/2010/@@");
test("stat $MP/store/time:/year/eq/2010/@@/file8");
test("ls -la $MP/store/time:/year/gt/2005/@@");
out_test('file8');

test("stat $MP/store/time:/year/gt/2005/@@/*___file8");
test("mkdir $MP/relations/tag1/is_equivalent/tag2");
test("mkdir $MP/relations/tag2/includes/tag3");
test("ls $MP/relations/tag1/@@/");
test("ls $MP/relations/tag1/@/*___file");

test("mkdir $MP/store/tag4/");
test("cp /tmp/file9 $MP/store/tag1/tag4/@");
test("stat $MP/store/tag1/@/file9");
test("mkdir $MP/relations/tag1/excludes/tag4");
test("stat $MP/store/tag1/@/file9");

goto OUT;

# ---------[no more test to run]---------------------------------------- <---
OUT:

print "\n" x 80;
print "*" x 80;
print "\nTests done! $tc test run - $tc_ok test succeeded - $tc_error test failed, summary follows:\n";

print $error_stack;

print "\n press [ENTER] to umount tagsistant...";
<STDIN>;

EXITSUITE: stop_tagsistant();
$TID->join();

exit();

# ---------[script end, subroutines follow]-----------------------------

#
# the core of the thread running tagistant, invoked
# by start_tagsistant()
#
sub run_tagsistant {
	print "Mounting tagsistant: $MCMD...\n";
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
	# check if user is part of fuse group
	#
	my $id = qx|id|;
	unless ($id =~ /\($FUSE_GROUP\)/) {
		die("User is not in $FUSE_GROUP group and is not allowed to mount a tagsistant filesystem");
	}

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
	sleep(3);

	#
	# check if thread was properly started
	#
	unless (defined $TID and $TID) {
		die("Can't create tagsistant thread!\n");
	}
	print "Testbed running!\n";
}

sub stop_tagsistant {
	print "\nUnmounting tagsistant: $UMCMD...\n";
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
	#my $command = join(" ", @_);
	my $command = shift();

	#
	# expected exit status
	#
	my $expected_exit_status = shift() || 0;

	#
	# run the command and trap the output in a global variable
	#
	$output = qx|$command 2>&1|;

	#
	# guess the operation status (OK or ERROR!)
	#
	my $cmdexit = $? >> 8;
	my $status = ($cmdexit == $expected_exit_status) ? "[  OK  ]" : "[ERROR!] ($cmdexit)";

	#
	# print summary
	#
	my $status_line = "\n____ $status [#$tc] $command " . "_" x (60 - length($tc) - length($command)) . "cmd__\n\n$output";
	print $status_line;
	$error_stack .= $status_line if $status =~ /ERROR/;

	#
	# return 0 if everything went OK
	#
	if ($status =~ /OK/) {
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
	my $got_an_error = 0;
	$tc++;
	for my $rx (@_) {
		$stc++;
		unless ($output =~ m/$rx/m) {
			$got_an_error++;
			$status = "[ERROR!]";
		} else {
			$status = "[  OK  ]";
		}
		my $status_line = "\n \\__ $status [#$tc.$stc] /$rx/ " . "_" x (56 - length($tc) - length($stc) - length($rx)) . "__re__\n";
		print $status_line;
		$error_stack .= $status_line if $status =~ /ERROR/;
	}

	if ($got_an_error) {
		$tc_error++;
	} else {
		$tc_ok++;
	}
	return 0;
}
