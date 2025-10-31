#!perl

# Parse the DuckDB header (duckdb.h)

# Copyright (c) 2025 Giuseppe Di Terlizzi
# SPDX-License-Identifier: Artistic-2.0


use strict;
use warnings;
use v5.10;
use utf8;

use Getopt::Long;
use Carp;

my %options = (header => 'duckdb.h', list => undef, ffi => undef, pod => undef);

GetOptions(\%options, 'header=s', 'list', 'ffi', 'pod') or Carp::croak('Error in command line arguments');

Carp::croak 'duckdb.h not found' unless -e $options{header};

return usage() if (!$options{list} && !$options{ffi} && !$options{pod});

open my $fh, '<', $options{header} or Carp::croak "$!";

my $is_c_api      = 0;
my $is_group      = 0;
my $is_deprecated = 0;

my $group_name = undef;
my $c_api      = undef;

my @GROUPS = ();
my %API    = ();


while (my $line = <$fh>) {

    chomp $line;

    next unless $line;

    if ($line =~ /\*\*DEPRECATED\*\*/) {
        $is_deprecated = 1;
    }

    if (!$is_group && $line =~ /\/\/===---/) {
        $is_group = 1;
        next;
    }

    if ($is_group && $line =~ /\/\/\s+/) {

        $group_name = $line;
        $group_name =~ s{// }{};

        $API{$group_name} //= [];
        push @GROUPS, $group_name;

        next;

    }

    if ($is_group && $line =~ /\/\/===---/) {
        $is_group = 0;
        next;
    }

    if ($line =~ /^DUCKDB_C_API/) {

        $c_api    = $line;
        $is_c_api = 1;

        if ($line =~ /\);/) {
            push @{$API{$group_name}}, {signature => $c_api, is_deprecated => $is_deprecated};
            $c_api         = undef;
            $is_c_api      = 0;
            $is_deprecated = 0;
            next;
        }

    }

    if ($line !~ /^DUCKDB_C_API/ && $is_c_api) {
        $line =~ s/^\s+//;
        $c_api .= " $line";
        if ($line =~ /\);/) {
            push @{$API{$group_name}}, {signature => $c_api, is_deprecated => $is_deprecated};
            $is_c_api      = 0;
            $is_deprecated = 0;
            $c_api         = undef;
        }
        next;
    }

}

close $fh;

sub usage {

    my $usage = <<"USAGE";
$0 - DBD::DuckDB::FFI utility

Usage:
    $0 --ffi
    $0 --pod
    $0 --list
USAGE

    say $usage;
    exit 0;

}

sub extract_signature {

    my $string = shift;

    my ($ret, $fn, $args) = $string =~ /DUCKDB_C_API\s([a-z0-9_ *]*)(duckdb_[a-z0-9_]*)\((.*)\);/;

    $args =~ s/(^\s+|\s+$)//g;
    $ret  =~ s/(^\s+|\s+$)//g;

    my @args = map { $_ =~ s/(^\s+|\s+$)//; $_ } split /,/, $args;

    my %output = (function => $fn, args => \@args, return => $ret);

    return wantarray ? %output : \%output;

}


sub build_pod {

    foreach my $group_name (@GROUPS) {

        next unless @{$API{$group_name}};

        say "=head2 $group_name\n";

        say "=over\n";

        foreach (@{$API{$group_name}}) {

            my %signature = extract_signature($_->{signature});

            my $fn = $signature{function};

            die $c_api if $fn =~ /\*/;

            say "=item * $fn\n";

        }

        say "=back\n";

    }

}

sub build_ffi {

    say "my \%DUCKDB_FUNCTIONS = (";

    foreach my $group_name (@GROUPS) {

        next unless @{$API{$group_name}};

        say "\n\t# $group_name\n";

        foreach (@{$API{$group_name}}) {

            my %signature = extract_signature($_->{signature});

            my $fn   = $signature{function};
            my $ret  = $signature{return};
            my $args = $signature{args};

            $ret = 'string' if $ret eq 'const char *';
            $ret = 'string' if $ret eq 'char *';
            $ret = 'opaque' if $ret =~ /\*/;

            my @ffi_args = ();

            foreach my $arg (@{$args}) {

                my ($type, $name) = split /\s/, $arg;

                $type .= '*' if ($arg =~ /\*/);
                $type = 'opaque'  if ($arg =~ /\*/ && $arg !~ /duckdb/);
                $type = 'string'  if ($arg =~ /char \*/);
                $type = 'string*' if ($arg =~ /char \*\*/);

                push @ffi_args, "'$type'";

            }

            my $ffi_args = join ', ', @ffi_args;

            say "\t$fn => [[$ffi_args] => '$ret']," . ($_->{is_deprecated} ? ' # DEPRECATED' : '');

        }

    }

    say ");";

}

sub list_fn {

    foreach my $group_name (@GROUPS) {

        next unless @{$API{$group_name}};

        foreach (@{$API{$group_name}}) {

            my %signature = extract_signature($_->{signature});

            my $fn = $signature{function};

            say $fn;

        }

    }

}


build_ffi if ($options{ffi});
build_pod if ($options{pod});
list_fn   if ($options{list});

exit 0;
