package Data::JPack;
use strict;
use warnings;
use feature ":all";

our $VERSION="v0.1.0";

use feature qw<say switch>;
no warnings "experimental";

use MIME::Base64;
use IO::Compress::RawDeflate qw<rawdeflate>;
use IO::Uncompress::RawInflate qw<rawinflate $RawInflateError>;

use File::Basename qw<basename>;
use File::ShareDir ":ALL";

use constant::more B64_BLOCK_SIZE=>(57*71); #Best fit into page size


my $share_dir=dist_dir "Data-JPack";

use Export::These qw<jpack_encode jpack_encode_file jpack_decode_file>;

# turn any data into locally (serverless) loadable data for html/javascript apps

#represents a chunk of a data to load
#could be a an entire file, or just part of one
#
use constant::more('options_=0', qw<compress_ buffer_ src_>);

sub new {
	my $pack=shift//__PACKAGE__;
  say STDERR "IN NEW JPACK";
	#options include
	#	compression
	#	tagName
	#	chunkSeq
	#	relativePath
	#	type
	#
	my $self=[];
	my %options=@_;
	$self->[options_]=\%options;;

	$self->[options_]{jpack_type}//="data";
	$self->[options_]{jpack_compression}//="none";
	$self->[options_]{jpack_seq}//=0;
  $self->[buffer_]="";
	bless $self , $pack;
}

sub encode_header {
	my $self=shift;
	for ($self->[options_]{jpack_compression}){
		if(/deflate/i){
			my %opts;
			my $deflate=IO::Compress::RawDeflate->new(\$self->[buffer_]);

			$self->[compress_]=$deflate;
		}
    else{
		}
	}

  # NOTE: Technically this isn't needed as the RawDefalte does not add the zlib
  # header. However if Deflate is used then this wipes out the header
  #
  $self->[buffer_]="";

	my $header=""; 
	my $options=($self->[options_]);
	if($self->[options_]{embedded}){
		$header.= ""
		. qq|<script defer type="text/javascript" onload="chunkLoaded(this)" |
		. join('', map {qq|$_="|.$options->{$_}.qq|" |} keys %$options)
		. ($self->[options_]{src}? qq|src="|.$self->[options_]{src}.qq|" >\n| : ">\n")
		;
	}

	$header.=""
  #. qq|console.log(document.currentScript);|
		. qq|chunkLoader.decodeData({jpack_path:document.currentScript.src,|
		. join(", ", map {qq|$_:"|.$options->{$_}.qq|"|} keys %$options)
		. qq|}, function(){ return "|;
		;
}

sub encode_footer {
	#force a flush
	my $self=shift;

  # flush internal buffer
  $self->[compress_]->flush() if $self->[compress_];
  # Encode the rest of the the data
  my $rem=encode_base64($self->[buffer_], "" );

	my $footer= $rem .qq|"\n});\n|;


	if($self->[options_]{embedded}){
		$footer.=qq|</script>|;
	}
	$footer;
}

sub encode_data {
	my $self=shift;
  my $data=shift;
  my $out="";
	if($self->[compress_]){
		$self->[compress_]->write($data);
	}
	else {
    # Data might not be correct size for base64 so append
		$self->[buffer_].=$data;
	}
	
  my $multiple=int(length ($self->[buffer_])/B64_BLOCK_SIZE);
  #
  #
  if($multiple){
    # only convert block if data is correcty multiple
   $out=encode_base64(substr($self->[buffer_], 0, $multiple*B64_BLOCK_SIZE,""),"");
  }
  $out;
}


sub encode {
  my $self=shift;
  my $data=shift;

	$self->encode_header
	.$self->encode_data($data)
	.$self->encode_footer
}

#single shot.. non OO
sub jpack_encode {
	my $data=shift;
	my $jpack=Data::JPack->new(@_);

	$jpack->encode($data);
}


sub jpack_encode_file {
	local $/;
	my $path = shift;
	return unless open my $file, "<", $path;
	jpack_encode <$file>, @_;
}

sub decode {
  my $self=shift;
  my $data=shift;
  my $compression; 
  $data=~/decodeData\(\s*\{(.*)\}\s*,\s*function\(\)\{\s*return\s*"(.*)"\s*\}\)/;
  my $js=$1;
  $data=$2;
  my @items=split /\s*,\s*/, $js;
  my %pairs= map {s/^\s+//; s/\s+$//;$_ }
          map {split ":", $_} @items;
  for(keys %pairs){
    if(/compression/){
      $pairs{$_}=~/"(.*)"/;
      $compression=$1;
    }
  }

  my $decoded;
  my $output="";
  for($compression){
    if(/deflate/){
      $decoded=decode_base64($data);
      rawinflate(\$decoded, \$output) or die $RawInflateError;
    }
    else {
      $output=decode_base64($data);
    }
  }
  $output;

}

sub jpack_decode {

}

sub jpack_decode_file {
	local $/;
	my $path=shift;
	return unless open my $file,"<", $path;
	my $data=<$file>;

  my $jpack=Data::JPack->new;
  $jpack->decode($data);
}


# returns a list of input paths to site output relative pairs suitable for builing a webbase client
sub resource_map {
  say STDERR "In resouce map";
  my @inputs=_bootstrap();

  #<$share_dir/js/*>;
  #
  my @outputs;

  push @outputs, "data/jpack/bootstrap.jpack";

  #############################################################
  # for(@inputs){                                             #
  #   if(/pako/){                                             #
  #     # Boot strap this into tmp dir                        #
  #     $_=_bootstrap();                                      #
  #     push @outputs, "data/jpack/bootstrap.jpack";          #
  #   }                                                       #
  #   else {                                                  #
  #     push @outputs, "client/components/jpack".basename $_; #
  #   }                                                       #
  # }                                                         #
  #############################################################
  map {($inputs[$_],$outputs[$_])} 0..$#inputs;
}

# Javascript resources absolute paths. These file are are to be copied into target output
sub js_paths {
  #use feature ":all";
  #say STDERR "Share dir is $share_dir";
  grep !/pako/, <$share_dir/js/*>;
}


# Encode the bootstrapping segment into a tempfile, return the path to this temp file
# This file contains the pako.js module as a chunk. Also prefixed with the
# chunkloader and worker pool contents
#
my $dir;
sub  _bootstrap {

    use File::Temp qw<tempdir>;
    $dir//=tempdir(CLEANUP=>1);

    my $data_file="$dir/bootstrap.jpack";

    return $data_file if -e $data_file;

    print STDERR "Regenerating  JPack bootstrap file\n";

    # If the file doesn't exist, create it
    my @pako=grep /pako/, <$share_dir/js/*>;


    my $packer=Data::JPack->new(jpack_compression=>undef, jpack_type=>"boot");

    my $pako= do {
        local $/=undef;
        open my $fh, "<", $pako[0]; #sys_path_src "client/components/jpack/pako.min.js";
        <$fh>;
    };

    # Process the contents of the chunkloader and workerpool scripts
    my @js=js_paths;
    my $prefix="";
    for(@js){
      $prefix.=do { open my $fh, "<", $_; local $/; <$fh>};
      $prefix.="\n";
    }

    # Pre encode pako into jpack format
    my $encoded=$packer->encode($pako);

    #do {
    open my $of, ">", $data_file; #"site/data/jpack/boot.jpack";

    print $of $prefix;
    print $of $encoded;

      #};
    $data_file;
}

1;
