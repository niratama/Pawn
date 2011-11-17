#!/usr/bin/env perl
package App::Pawn::script;
use 5.010;
use Getopt::Long;
use Term::ReadLine;
use strict;

use Net::OpenSSH;
use Net::OpenSSH::Constants qw(:error);
use Capture::Tiny qw(tee_merged);
use Time::Piece;
use Parallel::ForkManager;

$Capture::Tiny::TIMEOUT = 0;

sub new {
    my $class = shift;
    bless {
        argv => [],
        @_,
    }, $class;
}

sub parse_options {
    my $self = shift;
    local @ARGV = @{ $self->{argv} };
    push @ARGV, @_;

    Getopt::Long::Configure("bundling");
    Getopt::Long::GetOptions(
        'v|verbose' => \$self->{verbose},
        's|shell'   => \$self->{shell},
        'h|help'    => sub { $self->help; exit },
    );
    $self->{argv} = \@ARGV;
}

sub help {
    my $self = shift;
    print <<HELP;
Usage: pawn.pl [options] File

Optons:
  -h,--help       this message
  -s,--shell      simple shell
HELP
}

sub load_file {
    my $self   = shift;
    my $file   = shift @{ $self->{argv} };
    my $config = { file => $file };

    my $dsl = <<'DSL';
sub lx    { $self->local_exec(@_) };
sub lxr   { $self->local_exec_result(@_) };
sub sh    { $self->sh(@_) };
sub shr   { $self->sh_result(@_) };
sub rx    { $self->remote_exec($config->{host}, @_) };
sub rxr   { $self->remote_exec_result($config->{host}, @_) };
sub scp   { $self->scp($config->{host}, @_) };
sub rsync { $self->rsync($config->{host}, @_) };

sub logdir { my $e = shift || return $config->{logdir}; $config->{logdir} = $e }

sub hosts {
    my $g = shift || return $config->{hosts};
    return $config->{hosts}->{$g} unless @_;
    for my $host (@_) {
        if (ref($host) eq 'ARRAY') {
            my $fmt = shift @$host;
            push @{$config->{hosts}->{$g}}, map { sprintf($fmt, $_) } @$host;
        } else {
            push @{$config->{hosts}->{$g}}, $host;
        }
    }
}

sub commands {
    my $g = shift || return $config->{commands};
    my $c = shift || return $config->{commands}->{$g};
    if (ref($c) eq 'CODE') {
        $config->{commands}->{$g} = $c;
    } elsif (ref($config->{commands}->{$g}) eq 'CODE') {
        $config->{host} = $c;
        return $config->{commands}->{$g}->($config->{host}, @_);
    }
}

sub include {
    my $b = shift || return;
    my $f = dirname($file) . '/' . $b;
    unless ( do $f ) { die "can't include $f" }
}
DSL

    my $code = do { open my $io, "<", $file; local $/; <$io> };
    eval "package App::Pawn::Rule;\n"
      . "use 5.010;use File::Basename;\nuse strict;\nuse utf8;\n$dsl\n$code";
    die $@ if ($@);
}

sub loop {
    my $self = shift;
    if ( $self->{shell} ) {
        $self->shell;
    }
    else {
        $self->exec;
    }
}

sub shell {
    my $self  = shift;

    my $g = $self->{argv}->[0];
    say "shell:${g}" if $g;
    my $groups = App::Pawn::Rule::hosts();
    my $term  = Term::ReadLine->new('Pawn');
    my $out   = $term->OUT || \*STDOUT;
    while ( defined( my $line = $term->readline('Pawn> ') ) ) {
        next if $line =~ /^\s*$/;
        for my $group (keys %$groups) {
            next if ($group eq '_init') || ($group eq '_final');
            next if defined($g) && ($group ne $g);
            for my $host (@{$groups->{$group}}) {
                next unless ($host);
                my $ssh = Net::OpenSSH->new($host, strict_mode => 0);
                $ssh->error and die "Couldn't establish SSH connection: ". $ssh->error;
                my $output = $ssh->capture({ tty => 1 }, $line);
                printf $out "%s> %s\n", $host, $output;
            }
        }
    }
}

sub log {
    my $self = shift;
    my $time = shift;
    my $group = shift;
    my $host = shift;
    my $log = shift;

    my $logdir = App::Pawn::Rule::logdir() // '.';
    $logdir .= '/' . $time->datetime(date => '', T => '-', time => '');
    mkdir $logdir, 0755;
    open my $fd, '>', "${logdir}/${group}-${host}.log";
    print $fd $log;
    close $fd;
}

sub exec {
    my $self = shift;
    my $now = localtime;
    my $pm = Parallel::ForkManager->new(10);

    my $groups = App::Pawn::Rule::hosts();
    $self->log($now, '_init', 'localhost', tee_merged {
        App::Pawn::Rule::commands('_init', 'localhost', @{$self->{argv}});
    });
    for my $group (keys %$groups) {
        for my $host (@{$groups->{$group}}) {
            next unless ($host);
            $pm->start and next;
            my $log = tee_merged {
                App::Pawn::Rule::commands($group, $host);
                
            };
            $self->log($now, $group, $host, $log);
            $pm->finish;
        }
    }
    $pm->wait_all_children;
    $self->log($now, '_final', 'localhost', tee_merged {
        App::Pawn::Rule::commands('_final', 'localhost', @{$self->{argv}});
    });
}

sub local_capture {
  my $self = shift;
  open my $fd, '-|', @_ or die $!;
  my $out = join('', $fd->getlines);
  close($fd);
  return $out;
}

sub sh {
    my $self = shift;
    my @com = ('sh', '-c', join(' ', @_));
    say 'sh:' . join(' ', @com) if $self->{verbose};
    my $exitcode = system(@com);
    say 'result:' . $exitcode if $self->{verbose};
    return $exitcode;
}

sub sh_result {
    my $self = shift;
    my @com = ('sh', '-c', shift);
    say 'sh:' . join(' ', @com) if $self->{verbose};
    my $out = $self->local_capture(@com);
    say 'result:' . $out if $self->{verbose};
    return $out;
}

sub local_exec {
    my $self = shift;
    my @com = split /\s+/, shift;
    say 'local:' . join(' ', @com) if $self->{verbose};
    return system(@com);
}

sub local_exec_result {
    my $self = shift;
    my @com = split /\s+/, shift;
    say 'local:' . join(' ', @com) if $self->{verbose};
    my $out = $self->local_capture(@com);
    say 'result:' . $out if $self->{verbose};
    return $out;
}

sub remote_exec {
    my $self = shift;
    my $host = shift;
    my $ssh = Net::OpenSSH->new($host, strict_mode => 0);
    $ssh->error and die "Couldn't establish SSH connection: ". $ssh->error;
    my @com = split /\s+/, shift;
    say 'remote:' . join(' ', @com) if $self->{verbose};
    $ssh->system(@com);
    my $error = $ssh->error;
    my $result;
    if ((!$error) || ($error == OSSH_SLAVE_CMD_FAILED)) {
      $result = $?;
    }
    say 'result:' . $result if $self->{verbose};
    return $result;
}

sub remote_exec_result {
    my $self = shift;
    my $host = shift;
    my $ssh = Net::OpenSSH->new($host, strict_mode => 0);
    $ssh->error and die "Couldn't establish SSH connection: ". $ssh->error;
    my @com = split /\s+/, shift;
    say 'remote:' . join(' ', @com) if $self->{verbose};
    my $out = $ssh->capture(@com);
    say 'result:' . $out if $self->{verbose};
    return $out;
}

sub scp {
    my $self = shift;
    my $host = shift;
    my @com = split /\s+/, shift;
    unshift @com, ('scp');
    say 'scp:' . join(' ', @com) if $self->{verbose};
    return system(@com);
}

sub rsync {
    my $self = shift;
    my $host = shift;
    my @com = split /\s+/, shift;
    unshift @com, ('rsync');
    say 'rsync:' . join(' ', @com) if $self->{verbose};
    return system(@com);
}

sub doit {
    my $self = shift;
    $self->load_file;
    $self->loop;
}

package main;

unless (caller) {
    my $app = App::Pawn::script->new;
    $app->parse_options(@ARGV);
    $app->doit;
}
