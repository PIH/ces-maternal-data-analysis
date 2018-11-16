# CES Maternal Data Merge Tool

This is a tool for merging Censo Materna data with Partos data from Casa Materna
and Referencias data about pregnancies.


### Prerequisites

You'll need [Python 3](https://www.python.org/download/releases/3.0/),
pip (should come pre-installed with Python 3),
and Virtualenv (`pip3 install virtualenv`).


### Usage

First, export your .xlsx files to CSVs at
- `input/censo.csv`
- `input/partos-raw.csv`
- `input/refs-raw.csv`

Then, run `./preprocess.sh`.

If you haven't done so yet, run `./setup.sh`.

Then, run `./run.py`.

Results should appear in `output/`.


### Tuning

In `run.py`, look for the stuff under "Parameters". These numbers will change things
if you fiddle with them.

Also note the matcher being used by fuzzywuzzy (the matching
library). Matcher functions ("... Ratio") provided by fuzzywuzzy are documented in
the [fuzzywuzzy docs](https://github.com/seatgeek/fuzzywuzzy). They include e.g.
`token_sort_ratio` and `token_set_ratio`. As of this writing we're using a
combination, defined in `run.py`, called `combo_ratio`.


### Matching Logic

Censo and Partos are matched with each other, and then the result is matched with Refs.

To match rows, we go for each row in Censo...
- Identify candidate rows of Partos or Refs based on GA
	- greater than 0, less than 1 year
    - if GA can't be computed, assume it's a candidate
- Find the best name match from among those candidates
- If it's above the name match threshold, output a line with the combined data

Then, rows from Partos or Refs which correspond to the communities we're interested in
are added to the final dataset.

The order of operations is
1. Match Censo with Parto
2. Add Parto rows based on community
3. Match Censo/Parto combo with Refs
4. Add Refs rows based on community

