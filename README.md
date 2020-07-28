# STATA import for select variables from ZTRAX 
Stata files for importing specific fields from the Zillow ZTRAX database


**Purpose:**
This two part stata app selects specific variables from a selection of tables
in the ZTRAX database (ZASMT and ZTRANS), then subsets them to within a certain 
county, specified by FIPS code. It then cleans the data and outputs 
the selected transaction records as various .dta files for analysis.
	
**Computational needs:**
Loading in the largest tables (at least for state of Illinois) seems 
to require 32 GB of ram. Output file will be multible GB upon 
completion. Running this entire file, correctly specified, for Illinois and
subsetting for Cook County (FIPS 17031), takes 2.5 hours. 

**Important Notes:**
This program uses a sub function. Make sure both do files are available and the
sub-function is specified below

This program requires the 'missings' software package be installed.

The variables specified to keep in the various 'keepvars' fields are shortened
to account for limits on the length of varnames in Stata. Abbreviations currently
specified are:

		Borrower --> Brwr
		Original --> Orig
		Address  --> Addr
		Census   --> ''
