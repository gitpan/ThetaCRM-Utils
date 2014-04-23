package ThetaCRM::Utils;

use 5.010000;
use strict;
use warnings;

require Exporter;
use AutoLoader qw(AUTOLOAD);
use OpenOffice::OODoc;
odfLocalEncoding 'utf8';
use ConfigReader::Simple;
use English;
use Data::Dump qw(dump);
use DBD::Pg;
use File::HomeDir;
use Image::Math::Constrain;
use Image::Size;
use IPC::Open2;
use Perl6::Slurp;
use URI::Escape::XS qw(uri_escape);
use utf8;

use feature ':5.10';

our @ISA = qw(Exporter OpenOffice::OODoc);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use ThetaCRM::Utils ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw( sql_escape ) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw( 
	
);

our $VERSION = '0.01';

sub sql_escape ($);


# Preloaded methods go here.

# Autoload methods go after =cut, and are processed by the autosplit program.

sub new {
	my @args = @_;
	shift @args;
	my %args = @args;
	my $self  = {};
	bless $self;
	($self->{dbh}, $self->{'google_translation_api_key'}, $self->{'google_maps_api_key'}, $self->{markdown}, $self->{secure_server}, $self->{'pin'}, $self->{'editor'},
	 $self->{'sendmail'}, $self->{'dbname_remote'}, $self->{'host_remote'},
	 $self->{'username_remote'}, $self->{'password_remote'}, $self->{'port_remote'},
	 $self->{'ssh_remote'}, $self->{'ssh_username'}, $self->{'new_account'},
	 $self->{'pin_remote'}
 ) = $self->tcrm_connect(\%args);
	return $self;
}

sub sql_escape ($) {
	my ($self, $string) = @_;
	return unless $string;
	$string =~ s/'/''/g;
	$string =~ s/\\/\\\\/g;
	return $string;
}

sub tcrm_connect {
	my ($self, $args) = @_;
	my $HomeDir = File::HomeDir->my_home;
	my @config = qw(dbname host username password port sendmail
	dbname_remote host_remote username_remote password_remote port_remote
	ssh_remote ssh_username new_account pin_remote
	);
	my $config_file;
    if($$args{thetacrmrc}) {
		$config_file = $$args{thetacrmrc};
	} else {
		$config_file = $HomeDir . '/' . '.thetacrmrc';
	}	
	unless(-e $config_file) {
		die "Config file: $config_file missing!";
	}
	my $config = ConfigReader::Simple->new($config_file, [@config]);
	my $turnoff;
	foreach my $c (@config) {
		unless($config->exists($c)) {
			print "Configuration missing for $c.\n";
			$turnoff = 'yes';
		}
	}
	if($turnoff) { die "Configuration missing.\n" }
	my $dbname    = $config->get('dbname');
	my $dbname_remote = $config->get('dbname_remote');
	my $host      = $config->get('host');
	my $host_remote = $config->get('host_remote');
	my $username  = $config->get('username');
	my $username_remote = $config->get('username_remote');
	my $password  = $config->get('password');
	my $password_remote = $config->get('password_remote');
	my $port      = $config->get('port');
	my $port_remote = $config->get('port_remote');
	my $ssh_remote = $config->get('ssh_remote');
	my $ssh_username = $config->get('ssh_username');
	my $new_account = $config->get('new_account');
	my $pin_remote = $config->get('pin_remote');
	my $sendmail  = $config->get('sendmail');
	my $google_translation_api_key = $config->get('googletranslationapikey');
	my $google_maps_api_key = $config->get('googlemapsapikey');
	my $markdown = $config->get('default_markdown');
	my $editor = $config->get('editor');
	my $pin = $config->get('pin');
	my $secure_server = $config->get('secure_server');
	my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host",
		$username,
		$password,
		{AutoCommit => 1, RaiseError => 1, PrintError => 1, pg_enable_utf8 => 1, pg_bool_tf => 1}
	);
	return ($dbh, $google_translation_api_key, $google_maps_api_key, $markdown, $secure_server, 
		    $pin, $editor, $sendmail, $dbname_remote, $host_remote, $username_remote, 
			$password_remote, $port_remote, $ssh_remote, $ssh_username, $new_account, 
			$pin_remote
		);
		   
}

sub categories_list {
	my ($self, $parent, $type, $area) = @_;
	my $dbh = $self->{dbh};
	my $switch; 
	my $add;
	if($area && $area =~ /area/i) { 
		$switch = 'categories_area'; 
		$add = ' AND categories_parent IS NULL'; 
	} else { 
		$switch = 'categories_parent';
		$add = '';
   	}
	my $sql= qq{SELECT categories_id FROM categories, categorytypes WHERE $switch = $parent AND categorytypes_hid ~* '$type' AND categorytypes_id = categories_categorytypes $add};
	my $prepare = $dbh->prepare($sql);
	my $key = 'categories_id';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	my @row = keys %$hash_ref;
	return ( @row );
}

sub username_exists ($$) {
	my ($self, $username, $areas_id) = @_;
	my $dbh = $self->{dbh};
	if($username) {
		$username = $self->sql_escape($username);
	}
	my $sql = qq{SELECT usernames_id, usernames_datecreated, usernames_datemodified, usernames_username, usernames_usernamedb, usernames_password, usernames_contacts, usernames_areas, COALESCE(contacts_firstname, contacts_lastname) AS name FROM usernames, contacts, areas WHERE contacts_id = usernames_contacts AND areas_id = usernames_areas};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return 0 unless $row[0];
	return \@row;
}

sub count_page_realestate {
	# $a = categories_id
	my ( $self, $a ) = @_;
	my $dbh = $self->{dbh};
	my $command = qq{SELECT count(*) FROM realestateitems WHERE realestateitems_category0 = $a 
		OR realestateitems_category1 = $a OR realestateitems_category2 = $a 
		OR realestateitems_category3 = $a OR realestateitems_category4 = $a 
		OR realestateitems_category5 = $a OR realestateitems_category6 = $a
	   	OR realestateitems_category7 = $a OR realestateitems_category8 = $a 
		OR realestateitems_category9 = $a};
	my $result = $dbh->prepare($command);
	$result->execute();
	while (my @row=$result->fetchrow_array) {
		return $row[0];
	}
}

sub prepare_expose {
	my ($self, $template_dir, $pdfdir, $templateodt, $id, $realestate, $city, $title, $originals, $public, $expose, $imgmaxwidth, $shorttitle, $currency, $price, $m2, $description) = @_;
	if($currency eq 'BAM') { $price = $price . ' KM' }
	unless(-d $template_dir) { die "Template directory: $template_dir does not exist. Prepare template first.\n"; }
	unless($pdfdir) { $pdfdir = "~/PDF"; }
	unless(-d $pdfdir) { die "PDF directory of CUPS-PDF is missing.\n"; }
	unless(-r "$template_dir/$templateodt") { die "ODT template is missing.\n"; }
	binmode(STDOUT, ":utf8");
	binmode(STDIN, ":utf8");
	open(ODT, "<:encoding(UTF-8)", "$template_dir/$templateodt");
	my $file = join("\n", (<ODT>));
	close ODT;

	my $odffile = $template_dir . '/Template.odt';
	die "Missing odfDocument: $odffile\n" unless(-e $odffile);
	chdir($template_dir);
	my $archive = odfContainer('Templatenew.odt');
	my $doc = odfDocument(file => $archive);
	my $meta = odfMeta(file => $archive);
	#my $img = odfImage(file => $archive);
	my $subject = ucfirst($realestate) . ', ' . $shorttitle;
	my $keywords = ucfirst($realestate) . qq{, $shorttitle, $city, $price, 'Agencija Fortuna'};
	my $comment = qq{(C) 2010 by Vesna Dmitrović, Agencija "Fortuna"};
    utf8::encode($title);
    utf8::encode($realestate);
    utf8::encode($subject);
    utf8::encode($keywords);
    utf8::encode($comment);
    utf8::encode($price);
    utf8::encode($description);
	my $title2nd = ucfirst($shorttitle);
	if($m2) { $title2nd = $title2nd . ', ' . $m2 . ' m2' }
    utf8::encode($title2nd);
	$doc->appendParagraph( text => uc($realestate), style => 'Gunplay44');
	my $par2 = $doc->appendParagraph( text => $title2nd, style => 'Gunplay32');
	my $par3 = $doc->appendParagraph( text => '', style => 'Gunplay32');
	$meta->title($title);
	$meta->subject($subject);
	$meta->keywords($keywords);
	$meta->description($comment);
	my @expose = @{$self->find_expose_images($expose)};
	my $nr_expose = @expose;
	unless($nr_expose == 3) { 
		warn "Not enough expose pictures for real estate ID: $id.\n"; 
		return 0; 
	} else {
		my $image1 = $expose . '/' . $expose[0]; utf8::encode($image1);
		my $image1t = "Image 1"; utf8::encode($image1t);
		my $image2 = $expose . '/' . $expose[1];
		my $image3 = $expose . '/' . $expose[2];
		my $math1 = Image::Math::Constrain->new(400, 300);
		my ($w1, $h1) = imgsize($image1);
		my %t = %{$math1->constrain($w1, $h1)};
		my $size = $t{width} . 'pt, ' . $t{height} . 'pt';
		$doc->createImageElement( $image1t, 
			                   attachment => $par3, 
							   size => $size,
							   import => $image1 );
	}
	my $par4 = $doc->appendParagraph( text => $price, style => 'Gunplay32');
	my $par5 = $doc->appendParagraph( text => $description, style => 'Arial18B');
	my $par6 = $doc->setPageBreak($par5, position => 'after');
	
	$doc->save("TestFile.odt");
	
	chdir($template_dir);
	my $expose_filename = 'Expose-' . ucfirst($realestate) . '-' . $city . '-' . $id . '.pdf';
	$expose_filename =~ s/\s+/_/g;
	$expose_filename =~ s/_{2,}/_/g;
	#my $unoconv = qq{/usr/bin/unoconv --stdout TestFile.odt > TestFile.pdf};
	my $unoconv = qq{/usr/bin/unoconv --stdout TestFile.odt > "$expose/$expose_filename"};
	system($unoconv);
	return 1;
}

sub prepare_realestate_images {
	my ($self, $id, $realestate, $city, $title, $originals, $public, $expose, $imgmaxwidth) = @_;
	foreach my $dir ($originals, $public, $expose) { 
		unless(-d $dir) {
			die "Directory: $dir does not exist. Prepare directories first.\n";
		}
	}
	my @images = @{$self->find_dir_images($originals)};
	my @expose = @{$self->find_expose_images($originals)};
	foreach my $image (@expose) {
		my $source = $originals . '/' . $image;
		my $target = $expose    . '/' . $image;
		my $command = qq{/usr/bin/convert -format jpg -resize $imgmaxwidth "$source" "$target"};
		unless(-e "$target") { system($command) }
		my $comment = qq{$realestate, $city, $title, ID: $id};
		$self->jhead_comment($comment, $target);
	}

	foreach my $image (@images) {
		my $source = $originals . '/' . $image;
		my $target = $public    . '/' . $image;
		my $command = qq{/usr/bin/convert -format jpg -resize $imgmaxwidth "$source" "$target"};
		unless(-e "$target") { system($command) }
		my $comment = qq{$realestate, $city, $title, ID: $id};
		$self->jhead_comment($comment, $target);
	}
}

sub jhead_comment ($$) {
	my ( $self, $comment, $image ) = @_;
	die "jhead not in place /usr/bin/jhead" unless(-x '/usr/bin/jhead');
	my $command = qq{/usr/bin/jhead -cl '$comment' '$image' 2>&1 > /dev/null};
	system($command);
	if($CHILD_ERROR) {
		die "Could not execute system($command): signal is " . $CHILD_ERROR >> 8 . "\n";
	}
}


#===  FUNCTION  ================================================================
#         NAME:  mogrify_comment()
#      PURPOSE:  To put invisible comments inside of images.
#  DESCRIPTION:  It uses ImageMagick comment through system() call.
#   PARAMETERS:  $text to comment on the image and $image which is full path to the image
#      RETURNS:  status
#                
#                It shall be improved to find the mogrify in path.
#===============================================================================
sub mogrify_comment ($$) {
	my ( $self, $comment, $image ) = @_;
	$comment =~ s/\W/ /g;
	$comment =~ s/\s{2,}/ /g;
	my $command = qq{/usr/bin/mogrify -comment '$comment' '$image'};
	system($command);
	if($CHILD_ERROR) {
		die "Could not execute system($command): signal is " . $CHILD_ERROR >> 8 . "\n";
	}
}

sub find_expose_images {
	my ($self, $dir) = @_;
	unless(-d $dir) { die "Directory: $dir does not exist.\n" }
	opendir(DIR, $dir) || die "Cannot read directory: $dir.\n";
	my @images = grep { /\.(jpg|jpeg|png|tiff|gif)$/i && /exp/i } readdir(DIR);
	closedir DIR;
	return \@images;
}

sub find_dir_images {
	my ($self, $dir) = @_;
	unless(-d $dir) { die "Directory: $dir does not exist.\n" }
	opendir(DIR, $dir) || die "Cannot read directory: $dir.\n";
	my @images = grep { /\.(jpg|jpeg|png|tiff|gif)$/i } readdir(DIR);
	closedir DIR;
	return \@images;
}

sub realestate_page {
	my ($self, $realestateitem) = @_;
}

sub count_page_links {
	# $a = categories_id
	my ( $self, $a ) = @_;
	my $dbh = $self->{dbh};
	my $command = qq{SELECT count(*) FROM links WHERE (links_category0 = $a OR links_category1 = $a
		OR links_category2 = $a OR links_category3 = $a OR links_category4 = $a 
		OR links_category5 = $a OR links_category6 = $a OR links_category7 = $a
		OR links_category8 = $a OR links_category9 = $a) 
		AND links_approved IS TRUE};
	my $result = $dbh->prepare($command);
	$result->execute();
	while (my @row=$result->fetchrow_array) {
		return $row[0];
	}
}


sub insert_username ($$$$) {
	my ($self, $username, $password, $email, $areas_id) = @_;
	my $dbh = $self->{dbh};
	my $contacts_id;
	if($username) {
		for($username, $password, $email) {
			$_ = $self->sql_escape($_);
		}
		my $exist_email = $self->email_exists($email, $dbh);
		if($exist_email) {
			$contacts_id = $exist_email;
		} else {
			$contacts_id = $self->insert_contact_by_email($email, $areas_id);
		}
		my $sql = qq{INSERT INTO usernames (usernames_usernamedb, usernames_password};
	}
	return $contacts_id;
}

sub insert_contact {
	my ($self, $email, $areas_id) = @_;
	$email = $self->sql_escape($email);
	my $sql = qq{INSERT INTO };
}

sub insert_contact_by_account ($$) {
	my ($self, $account, $data) = @_;
	my $dbh = $self->{dbh};
	my %data = %{$data};
	my %account = %{$data{$account}};
	my $sql = qq{INSERT INTO contacts (contacts_lastname, contacts_account1, contacts_officephone, contacts_otherphone, contacts_fax, contacts_email1, contacts_email2, contacts_email3, contacts_website, contacts_primaryaddress, contacts_primarycity, contacts_primarypostalcode, contacts_primarystate, contacts_primarycountry) VALUES ( };
	if($account{accounts_email1}) {
		$sql .= "'" . $self->sql_escape($account{accounts_email1}) . "'";
	} elsif($account{accounts_email2}) {
		$sql .= "'" . $self->sql_escape($account{accounts_email2}) . "'";
	} elsif($account{accounts_phone}) {
		$sql .= "'" . $self->sql_escape($account{accounts_phone}) . "'";
	} elsif($account{accounts_phone2}) {
		$sql .= "'" . $self->sql_escape($account{accounts_phone2}) . "'";
	} else {
		$sql .= "'NEW'";
	}
	$sql .= ", ";

	$sql .= $account;
	$sql .= ", ";

	if($account{accounts_phone}) {
		$sql .= "'" . $self->sql_escape($account{accounts_phone}) . "'";
	} elsif($account{accounts_phone2}) {
		$sql .= "'" . $self->sql_escape($account{accounts_phone2}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_phone2}) {
		$sql .= "'" . $self->sql_escape($account{accounts_phone2}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_fax}) {
		$sql .= "'" . $self->sql_escape($account{accounts_fax}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_email1}) {
		$sql .= "'" . $self->sql_escape($account{accounts_email1}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_email2}) {
		$sql .= "'" . $self->sql_escape($account{accounts_email2}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_email3}) {
		$sql .= "'" . $self->sql_escape($account{accounts_email3}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_website}) {
		$sql .= "'" . $self->sql_escape($account{accounts_website}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_billingaddress}) {
		$sql .= "'" . $self->sql_escape($account{accounts_billingaddress}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_billingcity}) {
		$sql .= "'" . $self->sql_escape($account{accounts_billingcity}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_billingpostalcode}) {
		$sql .= "'" . $self->sql_escape($account{accounts_billingpostalcode}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_billingstate}) {
		$sql .= "'" . $self->sql_escape($account{accounts_billingstate}) . "'";
	} else {
		$sql .= 'NULL';
	}
	$sql .= ", ";

	if($account{accounts_billingcountry}) {
		$sql .= $self->sql_escape($account{accounts_billingcountry});
	} else {
		$sql .= 'NULL';
	}
	$sql .= ")";
	my $prepare = $dbh->prepare($sql);
	my $result  = $prepare->execute();
	my $contacts_id;
	if($result) {
		$contacts_id = $self->last_contact_of_account($account);
	} else {
		$contacts_id = undef;
	}
	return $contacts_id;
}

sub contacts_holdemail {
	my ($self, $eid, $id) = @_;
	foreach my $field (qw(contacts_account1 contacts_account2 contacts_account3)) {
		my $sql = "SELECT $field FROM contacts WHERE contacts_id = $id";
		my $ref = $self->{dbh}->selectall_hashref($sql, $field);
		my $accounts_id = (keys %$ref)[0]; # first value of hash 
		if($accounts_id) {
			my $update = "UPDATE mailingsubscriptions SET mailingsubscriptions_holdemail = TRUE WHERE mailingsubscriptions_accounts = $accounts_id AND mailingsubscriptions_contacts = $id";
			my $do_update = $self->{dbh}->do($update);
			my $insert = "INSERT INTO mailingsubscriptions (mailingsubscriptions_accounts, mailingsubscriptions_contacts, mailingsubscriptions_holdemail) SELECT $accounts_id, $id, TRUE WHERE NOT EXISTS (SELECT 1 FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $accounts_id AND mailingsubscriptions_contacts = $id)";
			my $do_insert = $self->{dbh}->do($insert);
		}
	}
	return unless $eid;
	my $mid = $self->mailinglist_by_eid($eid);
	warn "No mailings list (account) found" unless $mid;
	return unless $mid;
	my $update = "UPDATE mailingsubscriptions SET mailingsubscriptions_holdemail = TRUE WHERE mailingsubscriptions_accounts = $mid AND mailingsubscriptions_contacts = $id";
	my $insert = "INSERT INTO mailingsubscriptions (mailingsubscriptions_accounts, mailingsubscriptions_contacts, mailingsubscriptions_holdemail) SELECT $mid, $id, TRUE WHERE NOT EXISTS (SELECT 1 FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $mid AND mailingsubscriptions_contacts = $id)";
	my $do_update = $self->{dbh}->do($update);
	my $do_insert = $self->{dbh}->do($insert);
	return 1;
}

sub delete_all_mailings {
	my ($self, $contact_id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{DELETE FROM mailings WHERE mailings_contacts = $contact_id};
	my $result = $dbh->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub delete_last_mailing {
	my ($self, $contact_id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT mailings_id FROM mailings WHERE mailings_contacts = $contact_id ORDER BY mailings_id DESC LIMIT 1};
	my $hash_ref = $dbh->selectall_hashref($sql, 'mailings_id');
	my @k = keys %$hash_ref;
	if($k[0]) {
		my $del = qq{DELETE FROM mailings WHERE mailings_id = $k[0]};
		my $result = $dbh->do($del);
		return 0 if($result eq '0E0');
		return 1 if($result);
	} else {
		return 0
	}
}

sub last_contact_of_account {
	my ($self, $account) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT contacts_id FROM contacts WHERE contacts_account1 = $account ORDER BY contacts_id DESC LIMIT 1};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return $row[0] || undef;
}

sub mailingsubscription_exists {
	my ($self, $contacts_id, $accounts_id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT mailingsubscriptions_id FROM mailingsubscriptions WHERE mailingsubscriptions_contacts = $contacts_id AND mailingsubscriptions_accounts = $accounts_id};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return 0 unless $row[0];
	return $row[0];
}

sub accounts_exists {
	my ($self, $accounts_id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT accounts_id FROM accounts WHERE accounts_id = $accounts_id LIMIT 1};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return 0 unless $row[0];
	return $row[0];
}

sub email_exists {
	my ($self, $email) = @_;
	my $dbh = $self->{dbh};
	$email = $self->sql_escape($email);
	my $sql = qq{SELECT contacts_id FROM contacts WHERE contacts_email1 ~* '$email' OR contacts_email2 ~* '$email' OR contacts_email3 ~* '$email' ORDER BY contacts_id LIMIT 1};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return 0 unless $row[0];
	return $row[0];
}

sub get_table_oid {
	my ($self, $table) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname ~ '^($table)\$' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return $row[0] || undef;
}

sub links_allurls {
	my ($self, $id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT links_url FROM links};
	my $result = $dbh->prepare($sql);
	$result->execute();
	my @urls;
	while (my @row = $result->fetchrow_array) {
		push(@urls, $row[0]);
	}
	return \@urls;
}


sub links_by_thumbnail {
	my ($self, $id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT links_id, links_title, links_description, links_url, links_thumblast FROM links WHERE links_thumbnail IS TRUE AND links_category0  = $id };
	my $key = 'links_id';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub links_by_category {
	my ($self, $category, $search) = @_;
	my $dbh = $self->{dbh};
	#$search =~ s/\s+/|/g;
	if($search) {
		$search = $self->sql_escape($search);
		$search = "AND (links_title ~* E'$search' OR links_description ~* E'$search' OR links_url ~* '$search')";
	}
	my $sql = qq{SELECT links_id, links_title, links_description, links_url FROM links WHERE links_category0 = $category AND links_description ~ ' ' $search LIMIT 300};
	my $key = 'links_id';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub links_without_language_by_category {
	my ($self, $category) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT links_id, links_title, links_description FROM links WHERE links_category0 = $category AND links_language IS NULL AND (links_description ~ ' ')};
	my $key = 'links_id';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub get_dottelads {
	my ($self) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT * FROM dottelads WHERE dottelads_active IS TRUE};
	my $key = 'dottelads_id';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub get_words {
	my ($self) = @_;
	my $dbh = $self->{dbh};
	my $key = 'words_name';
	my $sql = qq{SELECT * FROM words};
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub insert_dotteldomain {
	my ($self, $domain, $id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{INSERT INTO dotteldomains (dotteldomains_dottelaccounts, dotteldomains_dotteldomain) VALUES ($id, '$domain')};
	unless($self->dotteldomain_exists($domain)) {
		my $result = $dbh->do($sql);
		return 0 if($result eq '0E0');
		return 1 if($result);
	} else {
		return 0;
	}
}

sub dotteldomain_exists {
	my ($self, $domain) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT dotteldomains_dotteldomain FROM dotteldomains WHERE dotteldomains_dotteldomain = '$domain'};
	my $result = $dbh->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub get_dottelaccounts {
	my ($self) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT * FROM dottelaccounts WHERE dottelaccounts_active IS NOT FALSE};
	my $key = 'dottelaccounts_id';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub dotteldomain {
	my ($self, $domain) = @_;
	die "No domain supplied" unless $domain;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT * FROM dotteldomains, dottelaccounts WHERE dotteldomains_dotteldomain = '$domain' AND dotteldomains_dottelaccounts = dottelaccounts_id};
	my $key = 'dotteldomains_dotteldomain';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub dotteldomain_language {
	my ($self, $domain) = @_;
	die "No domain supplied" unless $domain;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT dotteldomains_languages FROM dotteldomains WHERE dotteldomains_dotteldomain = '$domain'};
	my $result = $dbh->prepare($sql);
	$result->execute();
	my @row = $result->fetchrow_array;
	if($row[0]) {
		return $row[0];
	} else {
		warn "Could not find language by domain '$domain'\n";
		return 0;
	}
}

sub update_position {
	my($self, $domain, $plc, $remove) = @_;
	unless($domain && $plc) { return 0 }
	my $dbh = $self->{dbh};
	my %plc = %{$plc};
	my @ads;
	for my $placement (keys %plc) {
		if($remove) {
			push(@ads, 'dottelpositions_dottelads' . $placement . ' = NULL');
		} else {
			push(@ads, 'dottelpositions_dottelads' . $placement . ' = ' . $plc{$placement});
		}
	}
	my $adpos = join(", ", @ads); my $sql = qq{UPDATE dottelpositions SET $adpos FROM dotteldomains WHERE dotteldomains_id = dottelpositions_dotteldomains AND dotteldomains_keepempty IS FALSE};
    my $result = $dbh->prepare($sql);
    my $ntuples = $result->execute();
    if($ntuples eq '0E0') { return 0 } else { return 1 }
}

sub link_locations_latlon {
	my($self, $id) = @_;
	unless($id) { return 0 }
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT link_locations_latfloat, link_locations_lonfloat FROM link_locations WHERE link_locations_links = $id};
	my $result = $dbh->prepare($sql); 
	$result->execute();
	my @row = $result->fetchrow_array;
	if($row[0] && $row[1]) {
		return $row[0], $row[1]
	} else {
		return;
	}
}


sub link_locations_exists {
	my($self, $id) = @_;
	unless($id) { return 0 }
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT link_locations_links FROM link_locations WHERE link_locations_links = $id};
	my $result = $dbh->do($sql) || 0;
	return if($result eq '0E0');
	return 1 if($result);
}

sub dottelpages_data {
	my($self, $id) = @_;
	unless($id) { return }
	my $dbh = $self->{dbh};
	my $dottelpages = $self->dottelpages($id) || undef;
	my $dottelrecords = $self->dottelrecords($id) || undef;
	my $dottelkeywords = $self->dottelkeywords($id) || undef;
	return ($dottelpages, $dottelrecords, $dottelkeywords);
}

sub dottelpages {
	my($self, $id) = @_;
	unless($id) { return }
	my $dbh = $self->{dbh};
	my $key = 'dottelpages_id';
	my $sql = qq{SELECT * FROM dottelpages WHERE dottelpages_id = $id};
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub dottelrecords {
	my($self, $id) = @_;
	unless($id) { return }
	my $dbh = $self->{dbh};
	my $key = 'dottelrecords_id';
	my $sql = qq{SELECT * FROM dottelrecords WHERE dottelrecords_dottelpages = $id};
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub dottelkeywords {
	my($self, $id) = @_;
	unless($id) { return }
	my $dbh = $self->{dbh};
	my $key = 'dottelkeywords_id';
	my $sql = qq{SELECT * FROM dottelkeywords WHERE dottelkeywords_dottelpages = $id};
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub update_link_locations_raw {
	my($self, $id, $raw, $latfloat, $lonfloat) = @_;
	unless($id && $raw && $latfloat && $lonfloat) { return 0 }
	my $dbh = $self->{dbh};
	$raw = $self->sql_escape($raw);
	unless($self->link_locations_exists($id)) {
		my $sql = qq{INSERT INTO link_locations (link_locations_raw, link_locations_links, link_locations_latfloat, link_locations_lonfloat) VALUES ('$raw', $id, $latfloat, $lonfloat)};
		my $result = $dbh->do($sql) || 0;
		return 0 if($result eq '0E0');
		return 1 if($result);
	} else {
		my $sql = qq{UPDATE link_locations SET link_locations_raw = '$raw', link_locations_latfloat = $latfloat, link_locations_lonfloat = $lonfloat WHERE link_locations_links = $id};
		my $result = $dbh->do($sql) || 0;
		return 0 if($result eq '0E0');
		return 1 if($result);
	}
}

sub update_categories_description {
	my($self, $catid, $meta, $desc) = @_;
	unless($catid && $meta && $desc) { return 0 }
	my $dbh = $self->{dbh};
	$meta = $self->sql_escape($meta);
	$desc = $self->sql_escape($desc);
	my $sql = qq{UPDATE categories SET categories_description = E'$desc', categories_metadescription = E'$meta' WHERE categories_id = $catid};
    my $result = $dbh->prepare($sql);
    my $ntuples = $result->execute();
    if($ntuples eq '0E0') { return 0 } else { return 1 }
}

sub update_links_category {
	my ($self, $links_id, $links_category, $preference, $thumbnail, $page) = @_;
	unless($links_id && $links_category && $preference =~ /\d+/) { return 0 }
	unless($preference =~ /\d+/) { $preference = 0 }
	if($preference > 9 || $preference < 0) { return 0 }
	my $dbh = $self->{dbh};
	if($thumbnail) { $thumbnail = ', links_thumbnail = TRUE ' } else { $thumbnail = ''; }
	if($page) { $page = ', links_page = TRUE ' } else { $page = ''; }
	my $sql = qq{UPDATE links SET links_category$preference = $links_category $thumbnail $page WHERE links_id = $links_id};
	my $result = $dbh->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub update_links_thumblast {
	my ($self, $id) = @_;
	unless($id) { return 0 }
	my $dbh = $self->{dbh};
	my $sql = qq{UPDATE links SET links_thumblast = now() WHERE links_id = '$id' AND links_thumbnail IS TRUE};
	my $result = $dbh->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub update_dotteladditions_lastupdated {
	my ($self, $domain) = @_;
	unless($domain) { return 0 }
	my $dbh = $self->{dbh};
	my $sql = qq{UPDATE dotteladditions SET dotteladditions_lastupdated = now() FROM dotteldomains WHERE dotteldomains_dotteldomain = '$domain' AND dotteladditions_dotteldomains = dotteldomains_id};
	my $result = $dbh->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub update_css {
	my ($self, $domain, $css) = @_;
	unless($domain && $css) { return 0 }
	my $dbh = $self->{dbh};
	$css = $self->sql_escape($css);
	my $sql = qq{UPDATE dotteldomains SET dotteldomains_css = '$css' WHERE dotteldomains_dotteldomain = '$domain'};
    my $result = $dbh->prepare($sql);
    my $ntuples = $result->execute();
    if($ntuples eq '0E0') { return '.' } else { return 1 }
}

sub update_title {
	my ($self, $domain, $title) = @_;
	unless($domain && $title) { return 0 }
	my $dbh = $self->{dbh};
	$title = $self->sql_escape($title);
	my $sql = qq{UPDATE dotteldomains SET dotteldomains_title = '$title' WHERE dotteldomains_dotteldomain = '$domain' AND '$domain' != '$title'};
    my $result = $dbh->prepare($sql);
    my $ntuples = $result->execute();
    if($ntuples eq '0E0') { return '.' } else { return $title . "\n" }
}

sub time_passed {
	my ($self, $lastupdated, $interval, $after) = @_;
	my %interval = ( 1 => 'hour',
		             2 => 'day',
					 3 => 'week',
					 4 => 'month',
					 5 => 'year'
				 );

	$interval = "$after $interval{$interval}";
	unless($lastupdated && $interval) { return 0 }
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT 1 WHERE (SELECT now()) >= timestamp '$lastupdated' + interval '$interval'};
	my $result = $dbh->prepare($sql);
	$result->execute();
	my @row = $result->fetchrow_array;
	if($row[0] && ($row[0] eq 1)) {
		return 1;
	} else {
		return 0;
	}
}

sub get_dotteldomains {
	my ($self) = @_;
	my $dbh = $self->{dbh};
	my $key = 'dotteldomains_id';
	my $sql = qq{SELECT $key, dotteldomains_dotteldomain FROM dotteldomains WHERE dotteldomains_keepempty IS NOT TRUE ORDER BY dotteldomains_dotteldomain};
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	my @domains;
	foreach my $id (keys %$hash_ref) {
		push(@domains, $$hash_ref{$id}{'dotteldomains_dotteldomain'});
	}
	return \@domains;
}

sub get_dottelpositions {
	my ($self) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT dotteldomains_dotteldomain, dottelpositions_dotteldomains, dottelpositions_dotteladplacements1, dottelpositions_dotteladplacements2, dottelpositions_dotteladplacements3, dottelpositions_dotteladplacements4, dottelpositions_dotteladplacements5, dottelpositions_dotteladplacements6, dottelpositions_dotteladplacements7, dottelpositions_dotteladplacements8, dottelpositions_dotteladplacements9, dottelpositions_dotteladplacements10, dottelpositions_dotteladplacements11, dottelpositions_dotteladplacements12, dottelpositions_dottelads1, dottelpositions_dottelads2, dottelpositions_dottelads3, dottelpositions_dottelads4, dottelpositions_dottelads5, dottelpositions_dottelads6, dottelpositions_dottelads7, dottelpositions_dottelads8, dottelpositions_dottelads9, dottelpositions_dottelads10, dottelpositions_dottelads11, dottelpositions_dottelads12, dotteldomains_languages, dotteldomains_industries, dotteldomains_createifrequired, dotteldomains_keepempty, dotteladditions_dottelplugins, dotteladditions_intervals, dotteladditions_afterinterval, dotteladditions_lastupdated, dotteladditions_argument0, dottelpages_id, dottelpages_dotteldomains, dottelpages_title, dottelpages_description FROM dottelpositions, dotteldomains LEFT OUTER JOIN dotteladditions ON (dotteldomains.dotteldomains_id = dotteladditions.dotteladditions_dotteldomains) LEFT OUTER JOIN dottelpages ON (dottelpages.dottelpages_dotteldomains = dotteldomains.dotteldomains_id AND dottelpages.dottelpages_active IS TRUE) WHERE dotteldomains_id = dottelpositions_dotteldomains ORDER BY dotteldomains_dotteldomain};
	my $key = 'dotteldomains_dotteldomain';
	my $hash_ref = $dbh->selectall_hashref($sql, $key);
	return $hash_ref;
}

sub get_table_fields {
	my ($self, $table_oid) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT a.attname,
	  pg_catalog.format_type(a.atttypid, a.atttypmod),
	    (SELECT substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128)
		   FROM pg_catalog.pg_attrdef d
		      WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
			    a.attnotnull, a.attnum
				FROM pg_catalog.pg_attribute a
				WHERE a.attrelid = '$table_oid' AND a.attnum > 0 AND NOT a.attisdropped
				ORDER BY a.attnum
				};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my %data = %{$prepare->fetchall_hashref('attnum')};
	return \%data || undef;
}

sub contacts_with_phone {
	my ($self) = @_;
	my $sql = qq{SELECT contacts_id FROM contacts WHERE (contacts_officephone IS NOT NULL OR contacts_mobilephone IS NOT NULL OR contacts_homephone IS NOT NULL OR contacts_otherphone IS NOT NULL OR contacts_fax IS NOT NULL) ORDER BY contacts_id};
	my %t = %{$self->{dbh}->selectall_hashref($sql, 'contacts_id')};
	return \%t || undef;
}

sub contact_with_phone {
	my ($self, $contacts_id) = @_;
	my $sql = qq{SELECT contacts_id, contacts_firstname, contacts_lastname, contacts_officephone, contacts_mobilephone, contacts_homephone, contacts_otherphone, contacts_fax, contacts_primarycity, country_name(contacts_primarycountry) AS contacts_primarycountry FROM contacts WHERE contacts_id = $contacts_id};
	my %t = %{$self->{dbh}->selectall_hashref($sql, 'contacts_id')};
	return \%t || undef;
}

sub translation {
	my ($self, $words_id, $languages_id) = @_;
	my $sql = qq{SELECT translations_translation, translations_words FROM translations WHERE translations_words = $words_id AND translations_languages = $languages_id};
	my %t = %{$self->{dbh}->selectall_hashref($sql, 'translations_words')};
	return $t{$words_id}{'translations_translation'};
}

sub table_hash {
	my ($self, $table, $key, $search_key, $search_word, $integer) = @_;
	my $id = $table . '_id';
	my $sql = qq{SELECT * FROM $table};
    if($search_key && $search_word) {
		$search_word = $self->sql_escape($search_word);
		$search_word = "'" . $search_word . "'" unless $integer;
		$sql .= qq{ WHERE $search_key = $search_word };
	}	
	$sql .= qq{ ORDER BY $id};
	$key = $id unless $key;
	my $hash = $self->{dbh}->selectall_hashref($sql, $key);
	return $hash;
}


sub table_fieldnames ($) {
	my ($self, $table) = @_;
	my $table_oid = $self->get_table_oid($table);
	my %table_fields = %{$self->get_table_fields($table_oid)};
	my @order;
	foreach my $key (keys %table_fields) {
		push(@order, $table_fields{$key}{'attnum'});
	}
	my @fields;
	my @sorted = sort {$a <=> $b} @order;
	foreach my $nr (@sorted) {
		foreach my $key (keys %table_fields) {
			if($table_fields{$key}{'attnum'} == $nr) {
				push(@fields, $table_fields{$key}{'attname'}) unless $table_fields{$key}{'attname'} eq $table . '_id';
			}
		}
	}
	return \@fields;
}

sub get_table_data_by_md5 ($$) {
	my ($self, $table, $md5) = @_;
	my @fields = @{$self->table_fieldnames($table)};
	my $fields = join(', ', @fields);
	my $table_id = $table . '_id';
	$fields = $table_id . ', ' . $fields;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT $fields FROM $table WHERE md5(${table_id}::text) = $md5};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my %data = %{$prepare->fetchall_hashref($table_id)};
	return \%data || undef;
}

sub get_table_data_by_id ($$) {
	# returns record from any table by its id
	my ($self, $table, $id) = @_;
	my @fields = @{$self->table_fieldnames($table)};
	my $fields = join(', ', @fields);
	my $table_id = $table . '_id';
	$fields = $table_id . ', ' . $fields;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT $fields FROM $table WHERE $table_id = $id};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my %data = %{$prepare->fetchall_hashref($table_id)};
	return \%data || undef;
}

sub mailinglist_by_eid {
	my ($self, $eid) = @_;
	my $sql = "SELECT CASE WHEN emails_account IS NOT NULL THEN emails_account ELSE emails_mailinglist END AS account FROM emails WHERE emails_emailtypes = 2 AND emails_mailinglist IS NOT NULL AND emails_sent IS NOT TRUE AND emails_id = $eid";
	my $ref = $self->{dbh}->selectall_hashref($sql, 'account');
	my $id = (keys %$ref)[0]; # first value of hash 
	return $id;
}

sub unsubscribe {
	my ($self, $eid, $id) = @_;
	my $mid = $self->mailinglist_by_eid($eid);
	return unless $mid;
	my $update = "UPDATE mailingsubscriptions SET mailingsubscriptions_donotemail = TRUE, mailingsubscriptions_dateunsubscribed = current_timestamp WHERE mailingsubscriptions_accounts = $mid AND mailingsubscriptions_contacts = $id";
	my $insert = "INSERT INTO mailingsubscriptions (mailingsubscriptions_dateunsubscribed, mailingsubscriptions_accounts, mailingsubscriptions_contacts, mailingsubscriptions_donotemail) SELECT current_timestamp, $mid, $id, TRUE WHERE NOT EXISTS (SELECT 1 FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $mid AND mailingsubscriptions_contacts = $id)";
	my $do_update = $self->{dbh}->do($update);
	my $do_insert = $self->{dbh}->do($insert);
	return 1;
}


sub unsubscribe_old { # not used, it worked before, but now we have mailing subscriptions 
	my ($self, @emails) = @_;
	foreach my $email (@emails) {
		my ($sql, $result);
		$sql = "UPDATE contacts SET contacts_donotemail = TRUE WHERE ";
	    if($email =~ /@/) {
			$sql .= " contacts_email1 = '$email' 
				OR contacts_email2 = '$email' 
				OR contacts_email3 = '$email'";
		} elsif($email !~ /\D/) {
			$sql .= " contacts_id = $email";
		}
		$result = $self->{dbh}->do($sql);
		return 0 if($result eq '0E0');
		return 1 if($result);
	}
}

sub update_subscription {
	my ($self, $id, $mark, $email) = @_;
	return unless($mark =~ /donot|invalid|hold/);
	return unless($id || $email);
	my $sql;
    if($id) {
		$sql = 'UPDATE mailingsubscriptions SET mailingsubscriptions_invalidemail = TRUE WHERE mailingsubscriptions_contacts = $id';
	} elsif($email) {
		$sql = "UPDATE mailingsubscriptions SET mailingsubscriptions_invalidemail = TRUE WHERE mailingsubscriptions_email ILIKE '$email'";
	}
	my $result = $self->{dbh}->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub update_contact_email {
	my ($self, $id, $mark, $email) = @_;
	return unless($mark =~ /donot|invalid|hold/);
	return unless($id || $email);
	my $sql;
    if($email && $mark =~ /invalid/) {
		my @sql = (
			"UPDATE contacts SET contacts_invalid1 = TRUE WHERE contacts_email1 ILIKE '$email';",
			"UPDATE contacts SET contacts_invalid2 = TRUE WHERE contacts_email2 ILIKE '$email';".
			"UPDATE contacts SET contacts_invalid3 = TRUE WHERE contacts_email3 ILIKE '$email';"
		);
		foreach my $sql (@sql) {
			$self->{dbh}->do($sql);
		}
	}
}


sub get_contacts_id_by_email {
	my ($self, $email) = @_;
	my $sql = "SELECT contacts_id FROM contacts WHERE contacts_email1 ~* '$email' OR contacts_email2 ~* '$email' OR contacts_email3 ~* '$email'";
	my $h = $self->{dbh}->selectall_hashref($sql, 'contacts_id');
	my @i = keys %$h;
	my $nr = @i;
	if($nr > 1) {
		foreach my $id (@i) {
			warn "Found duplicates: $id";
		}
	}
	return($i[0]);
}

sub get_account_by_name {
	my ($self, $account) = @_;
	my $sql = "SELECT accounts_id FROM accounts WHERE accounts_name = '$account'";
	my $result = $self->{dbh}->prepare($sql);
	$result->execute();
	my @row = $result->fetchrow_array;
	return $row[0] || undef;
}

sub markdown {
	my ($self, $data, $processor) = @_;
	return $data unless $processor;
	#$SIG{CHLD} = 'IGNORE';
	my ($reader, $writer);
	my $pid = open2($reader, $writer, $processor);
	binmode($reader, ":utf8");
	binmode($writer, ":utf8");
	print $writer $data;
	close $writer;
	$data = slurp $reader;
	close $reader;
	waitpid($pid, 0);
	return $data;
}

sub get_emails {
	my ($self) = @_;
	my $sql = "SELECT emails_id, CASE WHEN emails_account IS NOT NULL THEN emails_account ELSE emails_mailinglist END AS account, emails_subject, emails_body, emails_mailinglist, emails_templates, emails_priority, emails_delay, emails_intervals, emails_languages FROM emails WHERE emails_emailtypes = 2 AND emails_mailinglist IS NOT NULL AND emails_sent IS NOT TRUE";
	my $hash = $self->{dbh}->selectall_hashref($sql, 'emails_id');
	return $hash;
}

sub mailing_list {
	my ($self, $id, $eid, $interval, $after) = @_;
	#say join(", ", $id, $eid, $interval, $after); 
	return unless $id;
	my %interval = ( 1 => 'hour',
		             2 => 'day',
					 3 => 'week',
					 4 => 'month',
					 5 => 'year'
				 );
	$interval = "$after $interval{$interval}";
	
	my $sql = "SELECT mailinglists_accounts FROM mailinglists WHERE mailinglists_mailinglist = $id";
	my $accounts = $self->{dbh}->selectall_hashref($sql, 'mailinglists_accounts');
	my @accounts = keys %$accounts;
	#say join(", ", @accounts); <STDIN>;
	push(@accounts, $id);
	my %list;
	foreach my $account (@accounts) {
		my $account = $account;
		if($account =~ /\D/) {
			$account = $self->get_account_by_name($account);
		} 

		my $sql = qq{WITH A AS 
		(SELECT contacts_id FROM contacts 
			WHERE (contacts_account1 = $account OR contacts_account2 = $account OR contacts_account3 = $account OR contacts_account1 = $id OR contacts_account2 = $id OR contacts_account3 = $id) 
			UNION SELECT mailingsubscriptions_contacts FROM mailingsubscriptions 
				WHERE mailingsubscriptions_accounts = $account 
			EXCEPT SELECT mailingsubscriptions_contacts FROM mailingsubscriptions 
				WHERE mailingsubscriptions_accounts = $account 
					AND (mailingsubscriptions_donotemail IS TRUE 
					OR mailingsubscriptions_holdemail IS TRUE) 
			EXCEPT SELECT mailingsubscriptions_contacts FROM mailingsubscriptions 
				WHERE mailingsubscriptions_accounts = $id 
					AND (mailingsubscriptions_holdemail IS TRUE) 
			EXCEPT SELECT mailings_contacts FROM mailings 
				WHERE mailings_emails = $eid 
			EXCEPT SELECT DISTINCT last_value(mailings_contacts) OVER (PARTITION BY mailings_contacts ORDER BY mailings_datecreated DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM mailings WHERE now() < (mailings_datecreated + interval '3 day')), B AS (SELECT A.contacts_id, contacts_firstname, contacts_lastname, CASE WHEN (contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) THEN contacts_email1 WHEN (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) THEN contacts_email2 WHEN (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE) THEN contacts_email3 END AS email FROM contacts, A WHERE ((contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) OR (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) OR (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE)) AND contacts.contacts_id = A.contacts_id) SELECT * FROM B; };
		$sql =~ s/\s+/ /g;


		# In this version, we did not put attention that if hold email is checked on one mailing
		# list that, email shall not be send to other mailing list, until unhold has been done
		#
#		my $sql = qq{WITH A AS (SELECT contacts_id FROM contacts WHERE (contacts_account1 = $account OR contacts_account2 = $account OR contacts_account3 = $account) UNION SELECT mailingsubscriptions_contacts FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $account EXCEPT SELECT mailingsubscriptions_contacts FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $account AND (mailingsubscriptions_donotemail IS TRUE OR mailingsubscriptions_holdemail IS TRUE) EXCEPT SELECT mailings_contacts FROM mailings WHERE mailings_emails = $eid EXCEPT SELECT DISTINCT last_value(mailings_contacts) OVER (PARTITION BY mailings_contacts ORDER BY mailings_datecreated DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM mailings WHERE now() < (mailings_datecreated + interval '3 day')), B AS (SELECT A.contacts_id, contacts_firstname, contacts_lastname, CASE WHEN (contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) THEN contacts_email1 WHEN (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) THEN contacts_email2 WHEN (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE) THEN contacts_email3 END AS email FROM contacts, A WHERE ((contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) OR (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) OR (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE)) AND contacts.contacts_id = A.contacts_id) SELECT * FROM B; };
		#say $sql;
#AND (contacts_id NOT IN (SELECT mailingsubscriptions_contacts FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $account AND 
#this line above is better solution for each mailing list to hold email separately, depending of SMTP discrimination
		# 
		# This one below is second version, depreciated as it is not scalable and much costly
#		my $sql = qq{SELECT DISTINCT contacts_id, contacts_firstname, contacts_lastname, 
#		CASE WHEN (contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) 
#				THEN contacts_email1 
#			WHEN (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) 
#				THEN contacts_email2 
#			WHEN (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE) 
#				THEN contacts_email3 END AS email 
#			FROM contacts 
#			WHERE ((contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) 
#				OR (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) 
#				OR (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE)) 
#				AND (contacts_account1 = $account OR contacts_account2 = $account OR contacts_account3 = $account) 
#				AND (contacts_id NOT IN (SELECT mailingsubscriptions_contacts FROM mailingsubscriptions WHERE mailingsubscriptions_accounts = $account AND 
#			         mailingsubscriptions_donotemail = TRUE))
#				AND (contacts_id NOT IN (SELECT mailingsubscriptions_contacts FROM mailingsubscriptions WHERE 
#			         mailingsubscriptions_holdemail = TRUE))
#				AND (
#					(contacts_id NOT IN (SELECT mailings_contacts FROM mailings WHERE mailings_contacts = contacts_id)) 
#					OR (
#						(contacts_id NOT IN (SELECT mailings_contacts 
#												FROM mailings 
#												WHERE mailings_emails = $eid AND contacts_id = mailings_contacts))
#					AND (contacts_id IN (SELECT mailings_contacts FROM mailings 
#											WHERE mailings_contacts = contacts_id 
#											AND now() >= (SELECT mailings_datecreated + interval '$interval' FROM mailings WHERE mailings_contacts = contacts_id ORDER BY mailings_datecreated DESC LIMIT 1)))
#										)
#					);};
#
# WORKING FIRST VERSION BELOW, not scalable, depreciated 
#			my $sql = qq{SELECT DISTINCT contacts_id, contacts_firstname, contacts_lastname, 
#		CASE WHEN (contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) 
#				THEN contacts_email1 
#			WHEN (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) 
#				THEN contacts_email2 
#			WHEN (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE) 
#				THEN contacts_email3 END AS email 
#			FROM contacts 
#			WHERE ((contacts_email1 IS NOT NULL AND contacts_invalid1 IS NOT TRUE) 
#				OR (contacts_email2 IS NOT NULL AND contacts_invalid2 IS NOT TRUE) 
#				OR (contacts_email3 IS NOT NULL AND contacts_invalid3 IS NOT TRUE)) 
#				AND (contacts_account1 = $account OR contacts_account2 = $account OR contacts_account3 = $account) 
#				AND contacts_donotemail IS NOT TRUE 
#				AND contacts_holdemail IS NOT TRUE 
#				AND (
#					(contacts_id NOT IN (SELECT mailings_contacts FROM mailings WHERE mailings_contacts = contacts_id)) 
#					OR (
#						(contacts_id NOT IN (SELECT mailings_contacts 
#												FROM mailings 
#												WHERE mailings_emails = $eid AND contacts_id = mailings_contacts))
#					AND (contacts_id IN (SELECT mailings_contacts FROM mailings 
#											WHERE mailings_contacts = contacts_id 
#											AND now() >= (SELECT mailings_datecreated + interval '$interval' FROM mailings WHERE mailings_contacts = contacts_id ORDER BY mailings_datecreated DESC LIMIT 1)))
#										)
#					);};
#say $sql; <STDIN>;
	my $row = $self->{dbh}->selectall_hashref($sql, 'contacts_id');
#		say $sql; 
#		say join(", ", keys %$row);
#		<STDIN>;
		%list = (%list, %$row);
	}
	foreach my $key (keys %list) {
		#say $key;
		unless($list{$key}{'email'}) {
			delete($list{$key});
		}
	}
	return \%list;	
}

sub last_email_sent {
	my ($self, $id) = @_;
	my $sql = qq{SELECT mailings_id, mailings_datecreated, mailings_contacts FROM mailings WHERE mailings_contacts = $id ORDER BY mailings_datecreated DESC LIMIT 1};
	my $hash = $self->{dbh}->selectall_hashref($sql, 'mailings_id');
	my $last;
	if((keys %$hash) && $$hash{(keys %$hash)[0]}{mailings_contacts}) {
		$last = $$hash{(keys %$hash)[0]}{mailings_datecreated};
	} else {
		$last = 0;
	}
	return $last;
}

sub check_mailing {
	my ($self, $contact, $type, $company, $subject, $id) = @_;
	my $sql;
	unless($id) {
		$sql = qq{SELECT mailings_id FROM mailings WHERE mailings_contacts = $contact AND mailings_mailingtypes = $type AND mailings_fromcompany = $company AND mailings_subject = '$subject'};
	} else {
		$sql = qq{SELECT mailings_id FROM mailings WHERE mailings_contacts = $contact AND mailings_mailingtypes = $type AND mailings_emails = $id};
	}
	my $hash = $self->{dbh}->selectall_hashref($sql, 'mailings_id');
	if(keys %$hash) {
		return 1
	} else {
		return 0
	}
}

sub record_mailing {
	my ($self, $contact, $type, $company, $subject, $description, $id) = @_;
	for($subject, $description) {
		$_ = $self->sql_escape($_);
		$_ = '' unless $_;
	}
	my $sql;
    unless($id) {
		$sql = qq{INSERT INTO mailings (mailings_contacts, mailings_mailingtypes, mailings_fromcompany, mailings_subject, mailings_description) VALUES ($contact, $type, $company, '$subject', '$description')};
	} else {
		$sql = qq{INSERT INTO mailings (mailings_contacts, mailings_mailingtypes, mailings_fromcompany, mailings_subject, mailings_description, mailings_emails) VALUES ($contact, $type, $company, '$subject', '$description', $id)};
	}
	my $result = $self->{dbh}->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub check_table_md5 ($$) {
	my ($self, $table, $md5) = @_;
	my $table_id = $table . '_id';
	my $dbh = $self->{dbh};
	my $sql = "SELECT $table_id FROM $table WHERE md5(${table_id}::text) = '$md5'";
	my $result = $dbh->prepare($sql);
	$result->execute();
	my @row = $result->fetchrow_array;
	return \$row[0] || undef;
}

sub area_navigation {
	my ($self, $area) = @_;
	my $sql = qq{SELECT pages_id, CASE WHEN pages_filename IS NOT NULL THEN pages_filename || filetypes_name(areas_defaultfiletype) ELSE pages_id || filetypes_name(areas_defaultfiletype) END FROM pages, areas WHERE areas_id = $area AND pages_category0 IS NULL AND pages_area = $area };
	my $dbh = $self->{dbh};
	my $hash = $dbh->selectall_hashref($sql, 'pages_id');
	foreach my $key (keys %$hash) {
		say $key;
	}
}

## Global Variables
sub varglob_givevalue {
	my ($self, $var) = @_;
	my $sql = qq{SELECT variables_value FROM variables WHERE variables_name = '$var' AND variables_vari
	abletypes = 3 LIMIT 1};
	my $dbh = $self->{dbh};
	my @var = @{$dbh->selectcol_arrayref($sql)};
	return $var[0];
}

sub varglob_exists {
	my ($self, $var) = @_;
	my $sql = qq{SELECT variables_id, variables_name FROM variables WHERE variables_name = '$var' AND variables_variabletypes = 3};
	my $dbh = $self->{dbh};
	my %ref = %{$dbh->selectall_hashref($sql, 'variables_id')};
	my $key = (keys %ref)[0];
	return 0 unless $key;
	return 1 if $key;
}

sub google_static_map {
	my ($self, $center, $zoom, $position, $language, $size) = @_;
	return unless $center;
	my %m;
	my $url = 'http://maps.googleapis.com/maps/api/staticmap?';
	my $alt = $center;
	utf8::decode($alt);
	$center = uri_escape($center);
	my $link = 'https://maps.google.com/maps?q=' . $center; # TODO if I want to link the image
	$center = '&center=' . $center;
	$zoom = 15 unless $zoom;
	$zoom = '&zoom=' . $zoom;
	$language = 'en' unless $language;
	$language = '&language=' . $language;
	$size = '400x400' unless $size;
	my ($width, $height) = split('x', $size);
	$size = '&size=' . $size;
	my $sensor = '&sensor=false';
	my $key = '&key=' . $self->{'google_maps_api_key'};
	$url .= $center . $zoom . $language . $size . $sensor; # TODO . $key;
	unless($position) {
		return $url;
	} elsif($position eq 'left' || $position eq 'right') {
		my $html = qq{<p>\n\t<img src="$url" style="float: $position; border-style: none; padding: 2em;" alt="$alt" title="$alt" width="$width" height="$height"/>\n</p>\n\n};
		return $html;
	} elsif($position eq 'center') {
		my $html = qq{<p style="text-align: center;">\n\t<img src="$url" style="border-style: none; padding: 2em;" alt="$alt" title="$alt" width="$width" height="$height"/>\n</p>\n\n};
	}
}


sub deep_navigation {
	my ($self, $class, $area, $activepage_id) = @_;
	$self->area_navigation($area);
	return '';
}

sub category_has_parent {
	my ($self, $id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{SELECT parent_category_id_by_category($id)};
	my $prepare = $dbh->prepare($sql);
	$prepare->execute();
	my @row = $prepare->fetchrow_array;
	return 0 unless $row[0];
	return $row[0];
}

sub set_main_page {
	my ($self, $pages_id) = @_;
	my $dbh = $self->{dbh};
	my $sql = qq{UPDATE pages SET pages_mainpage = TRUE WHERE pages_id = $pages_id};
	my $result = $dbh->do($sql);
	return 0 if($result eq '0E0');
	return 1 if($result);
}

sub edit {
	my ($self, $text, $description) = @_;
	return unless $text;
	if($description) {
		say "Editing: $description";
	}
	my $filename = `/bin/tempfile`;
	chomp $filename;
	open(FH, ">:encoding(UTF-8)", $filename);
	print FH $text;
	close FH;
	system("$self->{editor} '$filename'");
	my $ntext = slurp $filename, {utf8=>1};
	return $ntext;

}

sub self_or_default {
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

ThetaCRM::Utils - Utilities for ThetaCRM™ with CMS and ERP capabilities

=head1 SYNOPSIS

  use ThetaCRM::Utils;
  my $tcrm = ThetaCRM::Utils->new;

=head1 DESCRIPTION

Stub documentation for ThetaCRM::Utils, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.


=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

John Louis, E<lt>support1@thetabiz.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2002-2013 by THETABIZ S.A.

Artistic License 2.0

=cut
