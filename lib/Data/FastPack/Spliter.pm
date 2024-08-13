use strict;
use warnings;
use feature ":all";
no warnings "experimental";
package Data::FastPack::Spliter;
use version; our $VERSION=version->declare("v0.0.1");

use Data::Dumper;
use EV;
use AnyEvent;
use AnyEvent::AIO;
use IO::AIO;
use Promise::XS qw<deferred>;
use JSON;


use File::Spec::Functions qw<rel2abs>;

use Data::FIFO;
use Data::FastPack::Parser;
use Data::Base64;

use constant {
	INFIFO=>0,
	OUTFIFO=>1,
	OUTFILECOUNT=>2,
	BYTELIMIT=>3,
	MSGLIMIT=>4,
	BYTECOUNT=>5,
	MSGCOUNT=>6,
	CURRENTOUTFILE=>7,
	TARGETDIR=>8,
	DEF=>9,
	COMPRESSION=>10,
	TAGNAME=>11,
	RELATIVEDIR=>12,
	MANIFEST=>13,
	RELATIVEPATH=>14
};


sub new($pack=__PACKAGE__){
	my $o=[];
	bless $o,$pack;
}

sub init {
	my $self=shift;
	my $options=shift;
	$self->[DEF]=Promise::XS::deferred;
	$self->[OUTFILECOUNT]=0;
	$self->[BYTELIMIT]=50*1024*1024;
	$self->[MSGLIMIT]=-1;
	$self->[BYTECOUNT]=0;
	$self->[MSGCOUNT]=0;
	$self->[TARGETDIR]=$options->{targetDir};
	$self->[COMPRESSION]=$options->{compression};
	$self->[TAGNAME]=$options->{tagName};
	$self->[RELATIVEDIR]=$options->{relativeDir};
	$self->[MANIFEST]=[];
	$self->[BYTELIMIT]=$options->{byteLimit} if defined $options->{byteLimit};
	say "Spliter init complete";
}

#set the input fifo and make the spliter a sink
sub setFIFO($o,$fifo){
	$o->[INFIFO]=$fifo;
	Data::FIFO::setCallback $fifo, Data::FIFO::CB_NOTEMPTY, sub {
		#Process message
		_doMessage($o);
	};

	Data::FIFO::setCallback $fifo, Data::FIFO::CB_COMPLETE, sub {
		closeOutput($o)
		->then(sub {
				#write manifest
				say $o->[TARGETDIR]."/manifiest";
				open my $mfh,">",$o->[TARGETDIR]."/manifest";
				say $mfh encode_json $o->[MANIFEST];
				$o->[DEF]->resolve(1);
		})
	};
	say "setFIFO Complete";
}

#Entry point for each message
sub _doMessage($o){
	Data::FIFO::sinkBusy $o->[INFIFO];
	my $bLimit=$o->[BYTECOUNT]>=$o->[BYTELIMIT];
	my $sLimit=$o->[MSGCOUNT] > $o->[MSGLIMIT];
	$sLimit&&=$o->[MSGLIMIT]>0;
	my $fifo=$o->[INFIFO];

	if($bLimit || $sLimit){
		say "Scans, $o->[MSGCOUNT], bytes $o->[BYTECOUNT]";
		say "Limit reached. Closing file ", "b $bLimit, s $sLimit";
		$o->[MSGCOUNT]=0;
		$o->[BYTECOUNT]=0;
		closeOutput($o)
		->then( sub { say "ABOUT TO OPTNE OUTPUT";openOutput($o)})
		->then( sub { say "ABOUT TO WRITE OUTPUTS";writeOutput($o)})
		->then( sub { say "AOUT TO DO NEXT";
				Data::FIFO::sinkAvailable $o->[INFIFO];
	#Data::FIFO::next $fifo
			})
		->catch(sub {
				say "LKJSDF";
		})
	}
	else {
		unless(defined $o->[CURRENTOUTFILE]){
			openOutput($o)
			->then(sub {say "ABOUT TO WRITE OUTPUT"; writeOutput($o)})
			->then( sub {
					Data::FIFO::sinkAvailable $o->[INFIFO];
					#Data::FIFO::next $fifo
					});	
		}
		else {
			writeOutput($o)
			->then( sub {
					Data::FIFO::sinkAvailable $o->[INFIFO];
					#Data::FIFO::next $fifo
					});	
		}
	}

}

#Open a new output, write header
sub openOutput($o){
	say "OPEN";
	#my $cv=AE::cv;
	my $seq=$o->[OUTFILECOUNT];
	$o->[OUTFILECOUNT]++;
	my $path=rel2abs($o->[TARGETDIR]."/$seq.js");

	$o->[CURRENTOUTFILE]= Data::Base64::new;
	$o->[RELATIVEPATH]="$o->[RELATIVEDIR]/$seq.js";
	Data::Base64::open($o->[CURRENTOUTFILE], $path)
	->then( sub { 
			say "Writing header to output";
			Data::Base64::writeHeader $o->[CURRENTOUTFILE],{
				compression=>$o->[COMPRESSION],
				tagName=>$o->[TAGNAME],
				chunkSeq=>$seq,
				type=>"data",
				relativePath=>$o->[RELATIVEPATH]
			}
		}
	);
};

#Write footer and close
sub closeOutput($o) {
	say "CLOSE";
	#add file to the manifest
	push $o->[MANIFEST]->@*, $o->[RELATIVEPATH];
	Data::Base64::close $o->[CURRENTOUTFILE];
};

sub writeOutput($o){
	my $msg=Data::FIFO::pop $o->[INFIFO];#$entry->{Data::FIFO}; #POP and test if we could
	unless (defined $msg){
		my $def=deferred;
		$def->resolve([0,0]);
		return $def->promise();
	}
	#say  "length ".length $$msg;
	#say "SPLITTER :",Dumper $msg;
	return Data::Base64::writeToFromBuffer($o->[CURRENTOUTFILE], \$msg)
	->then(sub {
		$o->[MSGCOUNT]++;
		$o->[BYTECOUNT]+=$_[0]->[1];
		#say "byte count $o->[BYTECOUNT]";
		#say  "length ".length $msg;
		#$$msg=undef;
		$msg=undef;
		Promise::XS::resolved(1);
	});
}
1;
