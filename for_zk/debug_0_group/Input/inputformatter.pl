#!/usr/bin/perl

# run example: perl inputformatter.pl tinygraph 2 5

my $source_file= $ARGV[0];
my $file_num= $ARGV[1];
my $vertex_num= $ARGV[2];

open IN, "$source_file" or die "can't open $source_file!\n";
while (<IN>) { # $_
    if (/(\d+) (\d+)/) {
        $file_index = $1 % $file_num + 1;
        $edge_cnt[$file_index]++;
    }
}
close IN;

$file1_num = $vertex_num % $file_num;
for($i = 1; $i <= $file1_num; ++$i) {
    open $input_file[$i], "> ${source_file}_${file_num}w_$i" or die "can't open ${source_file}_${file_num}w_$i!\n";
    $total_vertex = $vertex_num / $file_num + 1;
    printf {$input_file[$i]} "%d\n", $total_vertex;
    printf {$input_file[$i]} "%d\n", $edge_cnt[$i];
}
$file1_num = $vertex_num % $file_num;
for($i = $file1_num + 1; $i <= $file_num; ++$i) {
    open $input_file[$i], "> ${source_file}_${file_num}w_$i" or die "can't open ${source_file}_${file_num}w_$i!\n";
    $total_vertex = $vertex_num / $file_num;
    printf {$input_file[$i]} "%d\n", $total_vertex;
    printf {$input_file[$i]} "%d\n", $edge_cnt[$i];
}

open IN, "$source_file" or die "can't open $source_file!\n";
while (<IN>) { # $_
    if (/(\d+) (\d+)/) {
        $file_index = $1 % $file_num + 1;
        print {$input_file[$file_index]} "$1 $2\n";
    }
}
close IN;
for($i = 1; $i <= $file_num; ++$i) {
    close $input_file[$i];
}
