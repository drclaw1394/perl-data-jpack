package Data::FastPack::Description;

# Represents a serialized description database.
# Provides ability to query the based on names, uuids, etc

use Cpanel::JSON::XS;
use Data::UUID:
use Data::Clone;

my %uuid_to_group_table;     # UUID to channel object mapping
my %uuid_to_channel_table;    #

my $ug    = Data::UUID->new;

# Encode to a string
sub encode {

  &encode_json;

}

# Decode from a string
sub decode {

  &decode_json;
  
}

sub add {
  my ($options,@items)=@_;

  for my $item (@items){
    my $clone=clone $item;
    if($options->{new_uuid}){
      $clone->{uuid}= $ug->create();
      #When we copy this value in we create a new UUID
    }
    else {
      # Reuse the exisint uuid
    }
    if($item->{uuid}){
      #
    }
  }
}

sub remove {

}

sub query {

}

sub new_entry {

}

1;
