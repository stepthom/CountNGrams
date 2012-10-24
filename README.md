CountNGrams version 0.01
========================

Use Apache Hadoop to count the n-grams in a collection of files.

AUTHOR
------

[Stephen W. Thomas](http://research.cs.queensu.ca/~sthomas/)
<<sthomas@cs.queensu.ca>>


DESCRIPTION
-----------

This class uses Hadoop to count the number of occurances of n-grams in a given
set of text files.

An n-gram is a list of n words, which are adjacent to each other in a text file. 
For example, if the text file contained the text "one two three", there would be
three 3 1-grams ("one", "two", "three"), 2 2-grams ("one_two", "two_three"), and
1 3-gram ("one_two_three").

This class has been tested against hadoop 1.0.3. 

N-grams of sizes 1, 2, 3, 4, and 5 are supported. You can specify on the command
line which you want to include via the third parameter (see below for details).


USAGE
-----

    countngrams input_dir output_dir 1,2,3,4,5?

    
See the HOWTO.txt for instructions to compile and run the application.



INSTALLATION
------------

See the HOWTO.txt for instructions to compile and run the application.


DEPENDENCIES
------------

This application depends on Hadoop 1.0.3 or variants thereof.


COPYRIGHT AND LICENCE
---------------------

Copyright (C) 2012 by Stephen W. Thomas <sthomas@cs.queensu.ca>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.10.1 or,
at your option, any later version of Perl 5 you may have available.




