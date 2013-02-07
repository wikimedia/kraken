-- Usage:
-- pig -p input=/path/to/data/files -p dest='/path/to/concat/file.tsv' -f concat_sort.pig

-- concat_sort.pig reeds in all data, sorts it by *, and then outputs it into
-- a single file.  This is useful for bringing together previously processed
-- and reduced data into a single file.

-- While concatenating, data will be stored into $output.tmp.  Once the data
-- has been stored, an existing $output directly will be removed, and
-- $output.tmp will be moved into its place.

-- Only run a single reducer, so that there will be a single file output
SET default_parallel 1;

-- remove any previously leftover $input.concat.tmp files.
fs -rm -r -f $input.concat.tmp

-- load and sort data
DATA = LOAD '$input';
DATA = ORDER DATA BY *;

-- output it into a single file in $output directory
STORE DATA into '$input.concat.tmp';

-- rename $input.concat.tmp/part-r-00000 to $dest.
fs -rm -f $dest;
fs -mv $input.concat.tmp/part-r-00000 $dest
fs -rm -r -f $input.concat.tmp
