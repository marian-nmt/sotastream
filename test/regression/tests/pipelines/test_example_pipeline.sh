#!/bin/bash

#####################################################################
# SUMMARY: Run 't2' pipeline with basic options
#####################################################################

# Exit on error
set -eo pipefail

# Test command
$AB_PYTHON -B -m sotastream --seed 1111 -b 1 -n 1 \
        example $AB_DATA/wmt22.en-de.tsv.gz $AB_DATA/wmt21.fr-de.tsv.gz \
    | head -n 1000 > example.out

# Compare with the expected output
diff example.out example.expected > example.diff

# Exit with success code
exit 0
