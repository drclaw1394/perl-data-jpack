package Data::FastPack::JPacker;
# Module for encoding Fastpack time series as JPACK files
# Also splits Fastpack files into smaller ones either on message count or byte limit
# 
use strict;
use warnings;
use feature "say";


use Data::FastPack;
use Data::JPack;
use Data::Dumper;
use File::Path qw<make_path>;
use File::Spec::Functions qw<rel2abs abs2rel>;
use File::Basename qw<basename dirname>;
use feature ":all";





#use constant KEY_OFFSET=>Data::JPack::Packer::KEY_OFFSET+Data::JPack::Packer::KEY_COUNT;
use enum ("byte_limit_="."0", qw<
	byte_size_
	message_limit_
	message_count_
	messages_
	html_root_
	html_container_
	jpack_
	jpack_options_
	write_threshold_
	read_size_
	ifh_
	out_fh_
	in_buffer_
	out_buffer_
	input_done_flag_
	jpack_flag_
	file_count_
	first_
	>);
#use constant KEY_COUNT=first_-byte_limit_+1;

sub new{
	my $package=shift//__PACKAGE__;
	my $self=[];
	$self->[messages_]=[];
	bless $self, $package;
	$self->init(@_);
}

#like new but reuse the object
sub init {
	my $self=shift;
		
	$self->[file_count_]=0;
	my %options=@_;
  $self->[html_container_]=$options{html_container}//"index.html";
  for($self->[html_container_]){
    $_.="/index.html" if -d;   # If container is actuall a dir, then make index
    $self->[html_root_]=rel2abs dirname $_;			# the html file which would be root
  }
	$self->[jpack_options_]=$options{jpack_options}//{};
	$self->[message_limit_]=$options{message_limit};
	$self->[byte_limit_]=$options{byte_limit};
	$self->[jpack_flag_]=1;
	$self->[first_]=1;
	$self->[in_buffer_]="";
	$self->[out_buffer_]="";
	$self->[read_size_]=$options{read_size}//4096*8;
	$self->[write_threshold_]=$options{write_size}//4096*8;



	#$self->[write_threshold_]=4096;
	mkdir $self->[html_root_] unless -e $self->[html_root_];
	$self;
}


sub close_output_file {
	my $self=shift;
	if($self->[out_fh_]){
		$self->[out_buffer_].=$self->[jpack_]->encode_footer if $self->[jpack_flag_];
		syswrite $self->[out_fh_], $self->[out_buffer_];
		close $self->[out_fh_];
		$self->[out_fh_]=undef;
		$self->[out_buffer_]="";
    $self->[file_count_]++;
	}
}

sub open_output_file {
	my $self =shift;
	my $dir= shift;
  # Format filename with up to 32 characters of hex (16 bytes/128 bits) That is
  # way more files than we currently can process, but does make it easy view
  # the file listing as the names are all the same length. It also makes it
  # very easy to load data as the browser simply keeps attempting to read
  # incrementally named files
	my $name=sprintf "$dir/%032x.jpack", $self->[file_count_];
	say STDERR "Opening output file: $name";
	open $self->[out_fh_], ">", $name;
	$name;
}
sub stats {
	my ($self)=@_;
	printf STDERR  "Message Count: %0d\n", $self->[message_count_];
	printf STDERR  "Byte Count: %0d\n", $self->[byte_size_];
	printf STDERR  "Buffer size: %0d\n", length $self->[out_buffer_];
	printf STDERR  "Input Done flag %s\n", $self->[input_done_flag_]?"YES":"NO";
	printf STDERR  "Message buffer %d\n", scalar $self->[messages_]->@*;
}

#Each call to this will pack all files passed.
#All files are added to current chunk group
#sequence numbers are applicable
#call reset if before this if data is unrelated
#return a list of packed files in group
#The returned files can be directly added to a container
#
# pack_files
# arguments are pairs of src and destinations. STDIN is a file with the path of "-"
# 
sub pack_files {
	my $self=shift;
	my @pairs=@_;	
	my @src= @pairs[map(((2*$_)),	 0..@pairs/2-1 )];	#files
	my @dst= @pairs[map(((2*$_)+1),	 0..@pairs/2-1)];
	say STDERR "SRCs: ", @src;	#Input FASTPACK data file
	say STDERR "DSTs: ", @dst;	#Directory for output files to live

	my $current_src;
	my $current_dst;
	my @outputs;
	my $message;

	my $start=time;
	my $now=time;
  my $previous_dst;

	while(){
    #say "";
		$now=time;
		if(($now-$start)>1){
			$start=$now;
			$self->stats;
		}
    
		#Parse input file and store messages in messages 
		if($self->[ifh_]){
      #say "have input file.. will read";
			my $read=sysread $self->[ifh_], $self->[in_buffer_], $self->[read_size_], length $self->[in_buffer_];
			if($read){
				#data present
        decode_message $self->[in_buffer_], $self->[messages_];
        #push $self->[messages_]->@*, $message while( $message=next_unparsed_message $self->[in_buffer_])

			}
			elsif($read==0){
        #say "END OF INPUT FILE: setting undef";
				$self->[ifh_]=undef;
			}
		}

    # Need to change input, but flush current messages to output first
    elsif($self->[out_buffer_] or $self->[messages_]->@*){
      #say "FLUSHING";
      #Flush
    }

		#Open a next input. No messages buffered at this point to safe to close/open outputfile
		else {
      #say "No buffer,  messages or open input file.. open next";
			#load new file
			if(@src){
        #say STDERR "Opening input file: $src[0]";
        $previous_dst= $current_dst;
				$current_src=shift @src;
				$current_dst=shift @dst;

        if($current_src eq "-"){
          
          say STDERR "Using standard input";
          $self->[ifh_]=\*STDIN;
        }
        else {
          say STDERR "Using $current_src input";
          open  $self->[ifh_], "<", $current_src; 
        }

        if(defined($previous_dst) and ($current_dst ne $previous_dst)){
          $self->close_output_file;
          $self->[file_count_]=0;
        }
        # Calculate the current file count
        my $p="$self->[html_root_]/$current_dst";
        my @list= map {hex} sort grep {length == 32 } map {s/\.jpack//; basename $_ } <$p/*.jpack>;
        say "found list: @list";
        $self->[file_count_]=((pop(@list)//0)+1);
        say "file count $self->[file_count_]";

        #say "Files at $current_dst: @list";
        say STDERR "CURRENT dst: $current_dst";
        #redo;  #need to read data
        $self->[first_]=1; # Reset the first message from file flag
			}
			else{
				#no more files or data to read
				$self->[input_done_flag_]=1;
			}
		}
	

		#Process messages in buffer
		while($self->[messages_]->@*){

			$message= shift $self->[messages_]->@*;
      #say Dumper  $message;
      #sleep 1;

			#Existing output file or size/count boundary reached
			if(
				!$self->[first_]
					and (!$self->[byte_limit_]||($self->[byte_size_]+$message->[FP_MSG_TOTAL_LEN])< $self->[byte_limit_])
					and (!$self->[message_limit_]|| ($self->[message_count_]<= $self->[message_limit_]))
			){
				#say "message: ", Dumper $message;


				#Serialse the messages and jpack encode
				if($self->[jpack_flag_]){
					my $buf="";
          encode_message $buf, [$message];
          #serialize_messages $buf, $message;
					my $data=$self->[jpack_]->encode_data($buf);
					#say $data if $data;
					$self->[out_buffer_].=$data;
				}

				#Just serialize...
				else {
          encode_message $self->[out_buffer_], [$message];
          #serialize_messages $self->[out_buffer_], $message;

				}
				$self->[message_count_]++;
				$self->[byte_size_]+=$message->[FP_MSG_TOTAL_LEN];
			}

			#No current output file of size/count boudary reached, or first message from file
			else {
				#close and open a file
				$self->close_output_file;
        #$self->[file_count_]++;
        
				my $out;
				$out=$self->[html_root_]."/$current_dst";
				my $dirname=$out;#dirname $out;
				my $basename=basename $out;

        #say STDERR "Making dir $dirname";
				make_path $dirname;
				my $f=$self->open_output_file($self->[html_root_]."/".$current_dst);
				#remove the abs html root
				$f=rel2abs( $f )=~s/^$self->[html_root_]\///r;
				push @outputs, $f;
				if($self->[jpack_flag_]){
					#create any dirs relative to container file

					$self->[jpack_options_]{jpack_seq}=$self->[file_count_];
          #$self->[jpack_options_]{jpack_path}=$f;#$current_dst;
					$self->[jpack_options_]{jpack_tag}=$basename;

					$self->[jpack_]=Data::JPack->new(
						$self->[jpack_options_]->%*

					);
				}


        if($self->[jpack_flag_]){
          # Encode into jpack format, after writing header
				  $self->[out_buffer_]=$self->[jpack_]->encode_header;

          my $buf="";
          encode_message $buf, [$message];
          my $data=$self->[jpack_]->encode_data($buf);
          $self->[out_buffer_].=$data;
        }
        else {
          # Directly append encoded fast pack message
          encode_message $self->[out_buffer_], [$message];
        }

				$self->[message_count_]=1;
				$self->[byte_size_]=$message->[FP_MSG_TOTAL_LEN];
				$self->[first_]=undef;
			}

			# write output file when the output buffer is full or input file is done.
			last if(
				(!$self->[ifh_] or length($self->[out_buffer_])>=$self->[write_threshold_])
			);
    }

    my $amount=length $self->[out_buffer_];
    while($amount){
      #say "WRITE";
      my $write=syswrite $self->[out_fh_], $self->[out_buffer_];
      substr $self->[out_buffer_], 0 ,$write,"";
      $amount-=$write;
    }
		
      
    if(!$self->[ifh_] and !$self->[messages_]->@*){
      #say "INPUT FINISHED, and messages flushed";
      $self->close_output_file;
    }


		last if $self->[input_done_flag_] and !$self->[messages_]->@*;
	}

        ######################################################################
        # #write any remaining data in the buffer                            #
        # while($self->[out_buffer_]){                                       #
        #         my $write=syswrite $self->[out_fh_], $self->[out_buffer_]; #
        #         substr $self->[out_buffer_],0 ,$write,"";                  #
        # }                                                                  #
        ######################################################################
	$self->close_output_file;
	@outputs
}
1;
