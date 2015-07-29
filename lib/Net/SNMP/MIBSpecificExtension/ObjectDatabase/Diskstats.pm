package Net::SNMP::MIBSpecificExtension::ObjectDatabase::Diskstats;
use 5.8.8;
use strict;
use warnings;
use base "Net::SNMP::MIBSpecificExtension::ObjectDatabase";

use constant INTEGER => "integer";
use constant STRING  => "string";
use constant COUNTER => "counter";
use constant GAUGE   => "gauge";

our $VERSION = "0.01";

my %DEFAULT = (
    TIME_TO_LIVE => 2 * 60,
    DATA_FILE    => "/proc/diskstats",
);
my @HEADINGS = qw(
    major_device_number  minor_device_number  device_name
    reads_completed  reads_merged  sectors_read  ms_spent_reading
    writes_completed  writes_merged  sectors_written  ms_spent_writing
    ios_currently_in_progress
    ms_spent_doing_io
    weighted_ms_spent_doing_io
);
my %TYPE = (
    major_device_number        => INTEGER,
    minor_device_number        => INTEGER,
    device_name                => STRING,
    reads_completed            => COUNTER,
    reads_merged               => COUNTER,
    sectors_read               => COUNTER,
    ms_spent_reading           => COUNTER,
    writes_completed           => COUNTER,
    writes_merged              => COUNTER,
    sectors_written            => COUNTER,
    ms_spent_writing           => COUNTER,
    ios_currently_in_progress  => GAUGE,
    ms_spent_doing_io          => COUNTER,
    weighted_ms_spent_doing_io => COUNTER,
);

sub data_file { shift->{data_file} }

sub time_to_live { shift->{time_to_live} }

sub init {
    my $self = shift;

    $self->{time_to_live} = $DEFAULT{TIME_TO_LIVE}
        unless defined $self->{time_to_live};
    $self->{data_file} = $DEFAULT{DATA_FILE}
        unless defined $self->{data_file};

    $self->{db} = { };
    $self->{dev_names} = [ ];

    $self->updated_at( 0 );

    return $self->SUPER::init;
}

sub updated_at {
    my $self = shift;
    if ( @_ ) {
        $self->{updated_at} = shift;
    }
    return $self->{updated_at};
}

sub does_update_needed {
    my $self = shift;
    my $now = time;

    return $now > $self->updated_at + $self->time_to_live;
}

sub read_data_file {
    my $self = shift;
    my @stats;

    open my $FH, "<", $self->data_file
        or die "Could not open a data file[", $self->data_file, "] for read: $!";
    chomp( my @lines = <$FH> );
    close $FH
        or die "Could not close a data file[", $self->data_file, "]: $!";

    for my $line ( @lines ) {
        $line =~ s{\A \s+ }{}msx;
        my %stat;
        my @values = split m{\s+}, $line;
        die "Could not parse line[$line]: headings count and values count are differ"
            if @values != @HEADINGS;
        @stat{ @HEADINGS } = @values;
        push @stats, \%stat;
    }

    return @stats;
}

sub __get_first_index {
    my( $list_ref, $target ) = @_;
    for ( my $i = 0; $i < @{ $list_ref }; $i++ ) {
        if ( $list_ref->[ $i ] eq $target ) {
            return $i;
        }
    }
    return;
}

sub update_database {
    my $self = shift;
    my $db_ref        = $self->db;
    my $dev_names_ref = $self->{dev_names};

    my @stats = $self->read_data_file;

    for my $stat_ref ( @stats ) {
        my $entry_index = __get_first_index( $dev_names_ref, $stat_ref->{device_name} );

        unless ( defined $entry_index ) {
            push @{ $dev_names_ref }, $stat_ref->{device_name};
            $entry_index = $#{ $dev_names_ref };
        }

        $entry_index++; # starts from 1.

        for ( my $i = 0; $i < @HEADINGS; $i++ ) {
            my $heading = $HEADINGS[ $i ];
            my $value = $stat_ref->{ $heading };
            my $object_index = $i + 2; # starts from 2., .1 is diskstatsIndex.

            my $oid = $self->base_oid . ".1.1.$object_index.$entry_index"; # 1 = diskstatsTable, and 1 = diskstatsEntry

            $db_ref->{ $oid }{value} = $value;
            $db_ref->{ $oid }{type} ||= $TYPE{ $heading }
                or die "Unknown type of heading[$heading] found.";
        }
    }

    $self->updated_at( time );

    return;
}

sub get {
    my $self = shift;
    my $oid  = shift;

    if ( $self->does_update_needed ) {
        $self->update_database;
    }

    return $self->SUPER::get( $oid );
}

sub __sort_oid {
    my( $lh, $rh ) = @_;
    $lh =~ s{\A [.] }{}msx;
    $rh =~ s{\A [.] }{}msx;
    $lh = join q{-}, map { sprintf "%03d", $_ } split m{[.]}, $lh;
    $rh = join q{-}, map { sprintf "%03d", $_ } split m{[.]}, $rh;
    return $lh cmp $rh;
}

sub get_next_oid {
    my $self = shift;
    my $oid  = shift;

    my @oids = sort { __sort_oid( $a, $b ) } keys %{ $self->db };
    my $index = __get_first_index( \@oids, $oid );

    return
        if !defined $index || $index + 1 >= @oids;

    return $oids[ $index + 1 ];
}

sub getnext {
    my $self = shift;
    my $oid  = shift;

    if ( $self->does_update_needed ) {
        $self->update_database;
    }

    return $self->SUPER::getnext( $oid );
}

sub dump_db {
    my $self = shift;
    my $db_ref = $self->db;
    my @oids = sort { __sort_oid( $a, $b ) } keys %{ $db_ref };
    my @lines;

    for my $oid ( @oids ) {
        push @lines, sprintf "%s = %s: %s", $oid, $db_ref->{ $oid }{type}, $db_ref->{ $oid }{value};
    }

    return join "\n", @lines;
}

1;

__END__
.{diskstatsTable}.{diskstatsEntry}.{diskstatsIndex}
.{diskstatsTable}.{diskstatsEntry}.{majorDeviceNumber}.
.{diskstatsTable}.{diskstatsEntry}.{minorDeviceNumber}.
.{diskstatsTable}.{diskstatsEntry}.{deviceName}.
.{diskstatsTable}.{diskstatsEntry}.{reads_completed}.
.{diskstatsTable}.{diskstatsEntry}.{reads_merged}.
.{diskstatsTable}.{diskstatsEntry}.{sectors_read}.
.{diskstatsTable}.{diskstatsEntry}.{ms_spent_reading}.

 202      16 xvdb 14210588 1025 973385149 34188613 5015331 98808 56848180 3680823 0 4044099 37856289
 202       0 xvda 22505084 17405473 1133586654 127017642 7626463 8616327 140812969 73498221 0 5587430 201278510
 202       1 xvda1 1372 0 10561 464 1046 0 4218 332 0 702 796
 202       2 xvda2 22503558 17405473 1133574861 127017116 7443708 8616327 140808751 73477819 0 5580984 201275936
  11       0 sr0 0 0 0 0 0 0 0 0 0 0 0
 253       0 dm-0 22184575 0 177476600 44540174 13773749 0 110189992 205424672 0 2276853 250031465
 253       1 dm-1 17819208 0 956097333 124467128 2364550 0 30618759 3316160 0 4066118 127788296
 253       2 dm-2 237614 0 3492903 97459 22284 0 386498 127828 0 89429 225294
 253       3 dm-3 11452193 0 892255891 31556320 3079594 0 49412506 3033723 0 3235715 34590818
 253       4 dm-4 2524016 0 77635459 2557987 206742 0 7049176 647891 0 849032 3206039
   7       0 loop0 0 0 0 0 0 0 0 0 0 0 0
   7       1 loop1 0 0 0 0 0 0 0 0 0 0 0
 253       5 dm-5 4385980 0 331487028 117896997 6355031 0 49335216 2354867 0 3669275 120252788
 253       7 dm-7 105650 0 10640386 5466005 185 0 1464 2072 0 192501 5468086
 253       6 dm-6 110597 0 8059738 5173671 553135 0 4425168 282183 0 234114 5455898
