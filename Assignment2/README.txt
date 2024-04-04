
----------

How to run our project:
	1. Download the zip file
	2. Open the CollocationExtraction program through IDE
	3. mvn clean + mvn package for all the step files (create jar files)
	3. Update the path variable according to the path where you saved the files
	4. Enter values for the environment variables (credentials)
	5. Run the program

----------

notes:
	* The calculation in the given formula is performed for each decade
	* To sort the list for calculating c(w2), we reversed the order of the word pair.

----------

Explanation of how our program works:
	step 1:
		* Filters pairs of words that are not relevant
		* Count all occurrences of the word pairs - ( * * n)
		* Count the number of occurrences for a word pair - (w1 w2 n)
		* Count the number of occurrences of the first word in the first position - (w1 * n)
	step 2:
		* Saves for each pair of words the relevant data for the calculation of the given formula - (c(w1,w2), c(N), c(w1))
	step 3: 
		* Count the number of occurrences of the second word in the second position - (* w2 n)
	step 4:
		* Calculates the PMI value of each pair of words - pmi(w1,w2) = log(c(w1,w2)) + log(N) - log(c(w1)) - log(c(w2))
		* Calculates the probability of each pair of words - p(w1,w2) = c(w1,w2) / N
		* Calculates the NPMI value of each pair of words - npmi(w1,w2) = pmi(w1,w2) / (-1*log(p(w1,w2))
	step 5:
		* Calculates the sum of the NPMI values in each decade
	step 6:
		* Filters the word pairs whose npmi does not meet the requirements
	step 7:
		* Sorts the word pairs by npmi size

----------

link to the output directory on S3: 

	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00000
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00001
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00004
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00005
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00006
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00007
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00008
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00009
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00010
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00011
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00012
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00013
	* https://bucket-collocation-extraction-eng.s3.amazonaws.com/output7/part-r-00014

	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00000
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00001
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00004
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00005
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00006
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00007
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00008
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00009
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00010
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00011
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00012
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00013
	* https://bucket-collocation-extraction-heb.s3.amazonaws.com/output7/part-r-00014
