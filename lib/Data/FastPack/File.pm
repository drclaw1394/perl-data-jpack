use strict;
use warnings;
use feature ":all";
no warnings "experimental";
package Data::FastPack::File;
use version; our $VERSION=version->declare("v0.0.1");

use Data::Dumper;
use EV;
use AnyEvent;
use AnyEvent::AIO;
use IO::AIO;
use Promise::XS qw<deferred>;

use Data::FIFO;
use Data::FastPack::Parser;

use constant do {
	my @keys=qw<GROUP
	FH
	BUFFER
	READOFFSET
	MSGCB
	ERRCB
	ENDCB
	FIFO
	BUSY
	TOTAL
	>;
	+{reverse %keys[0..$#keys]}
};

our $readSize=4096*32;

sub new($pack=__PACKAGE__){
	my $o=[];
	$o->[FIFO]=Data::FIFO::new;
	$o->[FIFO]->setCapacity(4);
	$o->[BUFFER]="";
	$o->[BUSY]=0;
	$o->[TOTAL]=0;
	return $o;
}

sub execute ($o){
	#Do the linking/callback setup
	Data::FIFO::setCallback $o->[FIFO], Data::FIFO::CB_NOTFULL, sub {
		readMessagesToFIFO($o);
	};

	Data::FIFO::setCallback $o->[FIFO], Data::FIFO::CB_START, sub {
		readMessagesToFIFO($o);
	};

	#kickstart the events
	Data::FIFO::start $o->[FIFO];
}

#This function is called to read from file, split messages/data and write to fifo
#File iot is donve via callbacks
sub readMessagesToFIFO ($io){
	#say caller;
	Data::FIFO::sourceBusy $io->[FIFO];
	#say "readMEssagesToFifo","Busy",$io->[FIFO];
	#return if $io->[FIFO]->[SOURCE_BUSY];	#
	_processMessageBuffer($io);
	#say "IS fifo not full ", $io->[FIFO]->notFull;
	if(Data::FIFO::notFull $io->[FIFO]){
		#say "will EXECUTE aio_Read";
		aio_read($io->[FH],undef,$readSize,$io->[BUFFER], length($io->[BUFFER]), sub {
				#say unpack "H*",$io->[BUFFER];
				#say "CB",Dumper \@_;
			unless ($_[0]){
				#ERROR or end here
				say "ERROR IN READ";
				Data::FIFO::stop $io->[FIFO];
				Data::FIFO::done $io->[FIFO];
				Data::FIFO::sourceAvailable $io->[FIFO];
				return;
			}
			$io->[TOTAL]=$io->[TOTAL]+$_[0];
			#say "TOTAL ", $io->[TOTAL];
			#Process each scan
			#Test if we have a message
			#_processMessageBuffer($io);
			readMessagesToFIFO($io); #NO FULL MESSAGE .. DO IT AGAIN

		});
	}
	else{
		#say "SOURCE AVAILABLE";
		Data::FIFO::sourceAvailable $io->[FIFO];
	}
}

#split the buffer into messages  and add to the fifo.
#only update the buffer once no more messages can be split out ie msg=undef;

sub _processMessageBuffer($io){
	#say "";
	my $msg=\1;
	local $,=", ";
	while(Data::FIFO::notFull($io->[FIFO]) and length $io->[BUFFER]){
		$msg=Data::FastPack::Parser::nextMessage(\$io->[BUFFER],\$io->[READOFFSET]);
		#say Dumper $msg;
		last unless defined $msg;
		#print defined($io->[BUFFER]),length($io->[BUFFER]),$io->[READOFFSET],"\n";
		Data::FIFO::unshift $io->[FIFO], $msg;

		#say "Adding msg: ", length $msg;
		#weaken $msg;
	}
	if(!defined($msg) and $io->[READOFFSET]>0){
		# Reset buffer and read offset. 
		#say "Updating buffer";
		#print defined($io->[BUFFER]),length($io->[BUFFER]),$io->[READOFFSET],"\n";
		$io->[BUFFER]=substr $io->[BUFFER],$io->[READOFFSET];
		$io->[READOFFSET]=0;
		#print defined($io->[BUFFER]),length($io->[BUFFER]),$io->[READOFFSET],"\n";
	}

}

sub open($o, $path){
	my $def=deferred;

	aio_open($path,IO::AIO::O_RDONLY, 0, sub {
			$o->[FH]=$_[0];
		$o->[GROUP]=undef;
		$o->[BUFFER]="";
		$o->[READOFFSET]=0;
		$_[0]?$def->resolve($o):$def->reject($o);
	});
	return $def->promise();
}

#Write a block from the fifo.
#Resolves promise when done
sub writeToFromFIFO($fh,$fifo){
	my $def=deferred();
	my $msg=Data::FIFO::pop $fifo;#$entry->{FIFO}; #POP and test if we could
	unless ($msg){
		$def->resolve([0,0]);
		return $def->promise();
	}

	#Check the scan and file size
	my $wCount=0;	
	my $write;
	$write=sub {
		aio_write($fh, undef, -$wCount +length $msg,$msg,$wCount,sub {
				die "Write error" if $_[0] ==-1 ;
				$wCount+=$_[0];
				if ($wCount < length $msg){
					&$write 
				}
				else {
					$def->resolve([1,length $msg]);
				}
		});
	};
	&$write;
	return $def->promise();
}
